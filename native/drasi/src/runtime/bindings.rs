use super::*;
use sha2::{Digest, Sha256};

fn public_subscription_id(id: &str) -> bool {
    id.strip_prefix("w-").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    })
}

impl WatchBinding {
    pub(super) fn subscription_id(&self) -> Result<String> {
        self.watch.validate()?;
        self.query.validate()?;
        if self.watch.namespace_id != self.query.namespace_id
            || self.watch_revision == 0
            || self.query_revision == 0
        {
            return Err(Error::new(
                1,
                "watch binding requires same-namespace lifetimes and exact public revisions",
            ));
        }
        let mut digest = Sha256::new();
        digest.update(b"temporal-native-watch-v1\0");
        digest.update(serde_json::to_vec(&self.watch)?);
        Ok(format!("w-{:x}", digest.finalize()))
    }
}

pub(super) fn validate_binding(
    catalog: &AdmissionLedger,
    subscription: &Subscription,
    binding: &WatchBinding,
) -> Result<()> {
    if binding.subscription_id()? != subscription.id || subscription.query_revision != 1 {
        return Err(Error::new(3, "watch binding native identity is invalid"));
    }
    let admission = catalog
        .nodes
        .get(&subscription.query_id)
        .ok_or_else(|| Error::new(3, "watch query is not retained in the admission catalog"))?;
    if admission.definition.lifetime != binding.query
        || !matches!(admission.definition.recipe, NodeRecipe::Query { .. })
        || admission.phase == Phase::Intended
    {
        return Err(Error::new(
            3,
            "watch query binding has the wrong lifetime, kind or admission phase",
        ));
    }
    Ok(())
}

pub(super) async fn validate_recovery(
    store: &dyn StateStoreProvider,
    config: &Config,
    catalog: &AdmissionLedger,
) -> Result<()> {
    let keys = store.list_keys(SUBSCRIPTIONS).await.map_err(io)?;
    if keys.len() > config.limits.subscriptions {
        return Err(Error::new(7, "persisted subscription capacity is invalid"));
    }
    for key in keys {
        let bytes = store
            .get(SUBSCRIPTIONS, &key)
            .await
            .map_err(io)?
            .ok_or_else(|| Error::new(7, "persisted subscription is missing"))?;
        let handoff: Handoff = serde_json::from_slice(&bytes)
            .map_err(|_| Error::new(7, "persisted subscription is invalid"))?;
        if handoff.definition.id != key {
            return Err(Error::new(7, "persisted subscription key is invalid"));
        }
        if let Some(binding) = &handoff.binding {
            validate_binding(catalog, &handoff.definition, binding)
                .map_err(|_| Error::new(7, "persisted public watch binding is invalid"))?;
        } else if public_subscription_id(&key) {
            return Err(Error::new(7, "persisted public watch binding is missing"));
        }
        if catalog
            .nodes
            .get(&handoff.definition.query_id)
            .is_some_and(|node| node.retirement.is_some())
            && (!handoff.deleted || handoff.pending.is_some() || handoff.results.is_some())
        {
            return Err(Error::new(
                7,
                "retired query has a live persisted subscription",
            ));
        }
    }
    Ok(())
}

impl Handoff {
    fn observation(&self) -> SubscriptionObservation {
        SubscriptionObservation {
            definition: self.definition.clone(),
            binding: self.binding.clone(),
            deleted: self.deleted,
        }
    }
}

impl Host {
    pub(super) async fn subscribe(
        &self,
        subscription: Subscription,
        binding: Option<WatchBinding>,
    ) -> Result<SubscriptionObservation> {
        identifier(&subscription.id)?;
        if binding.is_none() && public_subscription_id(&subscription.id) {
            return Err(Error::new(
                3,
                "public watch subscription requires its retained binding",
            ));
        }
        let query_config = self.query_config(&subscription.query_id).await?;
        if query_config.revision != subscription.query_revision {
            return Err(Error::new(
                3,
                "query revision does not match the subscription",
            ));
        }
        {
            let _handoffs = self.handoffs.lock().await;
            if self
                .has_subscription(&subscription, binding.as_ref())
                .await?
            {
                return Ok(self.handoff(&subscription.id).await?.observation());
            }
            self.require_admission_safe().await?;
            self.query_config(&subscription.query_id).await?;
        }
        let snapshot = self
            .snapshot(&subscription.query_id, &subscription.id)
            .await?;
        let _handoffs = self.handoffs.lock().await;
        if self
            .has_subscription(&subscription, binding.as_ref())
            .await?
        {
            return Ok(self.handoff(&subscription.id).await?.observation());
        }
        self.require_admission_safe().await?;
        self.query_config(&subscription.query_id).await?;
        if self.store()?.key_count(SUBSCRIPTIONS).await.map_err(io)?
            >= self.config.limits.subscriptions
        {
            return Err(Error::new(3, "subscription capacity exhausted"));
        }
        let handoff = Handoff {
            definition: subscription,
            binding,
            incarnation: snapshot.incarnation.clone(),
            cursor: snapshot.sequence,
            results: Some(
                snapshot
                    .rows
                    .iter()
                    .map(|row| (row.key.clone(), row.value.clone()))
                    .collect(),
            ),
            pending: Some(snapshot),
            last_receipt: None,
            deleted: false,
        };
        self.persist(&handoff).await?;
        Ok(handoff.observation())
    }

    pub(super) async fn list_subscriptions(&self, after: &str, limit: usize) -> Result<Value> {
        if limit == 0 || limit > 64 {
            return Err(Error::new(1, "subscription page limit must be in 1..64"));
        }
        if !after.is_empty() {
            identifier(after)?;
        }
        let _handoffs = self.handoffs.lock().await;
        let mut keys = self.store()?.list_keys(SUBSCRIPTIONS).await.map_err(io)?;
        keys.sort();
        keys.retain(|key| key.as_str() > after);
        let next = if keys.len() > limit {
            keys[limit - 1].clone()
        } else {
            String::new()
        };
        let mut observations = Vec::new();
        for id in keys.into_iter().take(limit) {
            observations.push(self.handoff(&id).await?.observation());
        }
        Ok(json!({"subscriptions": observations, "next_page_token": next}))
    }
}
