const assert = require('node:assert/strict');
const {
  CreateWatchRequest,
  GetPluginCapabilitiesRequest,
  InitialResults,
  OutputIdentity,
  PluginCapabilities,
  ResourceStatus,
  SecretReference,
  Source,
  Query,
  Watch,
  WatchSpec,
} = require('../.tools/ts-test/drasi/watch/v1/service.js');

const wire = {
  namespace: 'test',
  id: 'watch',
  requestId: 'create',
  spec: {
    query: { id: 'query', revision: '9007199254740993' },
    trigger: 'TRIGGER_ENTER',
    initialResults: 'INITIAL_RESULTS_INCLUDE',
    once: {
      owner: { workflowId: 'workflow', runId: '11111111-1111-4111-8111-111111111111' },
      deadline: '2030-01-01T00:00:00.000Z',
    },
    action: {
      signal: {
        run: { workflowId: 'workflow', runId: '11111111-1111-4111-8111-111111111111' },
        signalName: 'change',
        input: {
          payloads: [{
            metadata: { encoding: 'anNvbi9wbGFpbg==', custom: 'AAH/' },
            data: 'eyJhbW91bnQiOjd9',
          }],
        },
      },
    },
  },
};

const model = CreateWatchRequest.fromJSON(wire);
assert.equal(model.spec.lifetime.$case, 'once');
assert.equal(model.spec.action.operation.$case, 'signal');
assert.deepEqual(CreateWatchRequest.toJSON(model), wire);
assert.equal(WatchSpec.fromJSON({}).initialResults, InitialResults.INITIAL_RESULTS_IGNORE);

for (const Resource of [Source, Query, Watch]) {
  for (const desiredState of [
    'DESIRED_STATE_STOPPED',
    'DESIRED_STATE_RUNNING',
    'DESIRED_STATE_PAUSED',
    'DESIRED_STATE_DELETED',
    'DESIRED_STATE_CANCELED',
  ]) {
    const wire = { namespace: 'test', id: 'resource', revision: '9007199254740993', desiredState };
    const resource = Resource.fromJSON(wire);
    assert.equal(resource.status, undefined);
    assert.deepEqual(Resource.toJSON(resource), wire);
  }
}

const identity = {
  queryRevision: '9007199254740993',
  incarnation: 'incarnation',
  sequence: '9007199254740995',
  rowIdentity: 'row',
  diffOrdinal: 2,
};
assert.deepEqual(OutputIdentity.toJSON(OutputIdentity.fromJSON(identity)), identity);

for (const reference of [
  { provider: 'databases', key: 'replication' },
  { provider: 'databases', key: 'replication', version: 'generation/17:rotation' },
]) {
  assert.deepEqual(SecretReference.toJSON(SecretReference.fromJSON(reference)), reference);
}

for (const sequence of ['0', '37', '38']) {
  const status = ResourceStatus.fromJSON({ acceptedOutput: { ...identity, sequence } });
  const decoded = ResourceStatus.fromJSON(ResourceStatus.toJSON(status));
  assert.deepEqual(decoded, status);
  assert.equal(decoded.acceptedOutput.sequence, sequence);
}

assert.deepEqual(GetPluginCapabilitiesRequest.toJSON(GetPluginCapabilitiesRequest.fromJSON({})), {});
const pluginCapabilities = {
  enabled: true,
  apiVersion: 'v1',
  allowedMethods: ['/drasi.watch.v1.PluginService/InstallPlugin'],
  maxRequestBytes: 1048576,
  message: 'Plugin management is available',
};
assert.deepEqual(PluginCapabilities.toJSON(PluginCapabilities.fromJSON(pluginCapabilities)), pluginCapabilities);
const unavailableCapabilities = PluginCapabilities.fromJSON({});
assert.equal(unavailableCapabilities.enabled, false);
assert.deepEqual(unavailableCapabilities.allowedMethods, []);
console.log('Generated TypeScript JSON contract round trips passed.');
