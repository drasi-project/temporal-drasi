use crate::{
    credentials::{AdmissionValues, ComponentSlot, ResolvedField},
    protocol::{Error, Result, SecretRef, MAX_INPUT},
};

struct Packet<'a>(&'a [u8]);

impl<'a> Packet<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        if length > self.0.len() {
            return Err(Error::new(1, "truncated plugin credential packet"));
        }
        let (value, remaining) = self.0.split_at(length);
        self.0 = remaining;
        Ok(value)
    }

    fn length(&mut self) -> Result<usize> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize)
    }

    fn blob(&mut self, limit: usize) -> Result<&'a [u8]> {
        let length = self.length()?;
        if length > limit {
            return Err(Error::new(
                1,
                "plugin credential packet field exceeds its byte limit",
            ));
        }
        self.take(length)
    }

    fn text(&mut self, limit: usize) -> Result<String> {
        let value = std::str::from_utf8(self.blob(limit)?)
            .map_err(|_| Error::new(1, "plugin credential packet contains invalid metadata"))?;
        if value.trim().is_empty() || value.chars().any(char::is_control) {
            return Err(Error::new(
                1,
                "plugin credential packet contains invalid metadata",
            ));
        }
        Ok(value.into())
    }
}

pub(super) fn decode(bytes: &[u8]) -> Result<AdmissionValues> {
    if bytes.len() > MAX_INPUT {
        return Err(Error::new(
            1,
            "plugin credential packet exceeds its byte limit",
        ));
    }
    let mut packet = Packet(bytes);
    if packet.take(4)? != b"TDC1" {
        return Err(Error::new(
            1,
            "unsupported plugin credential packet version",
        ));
    }
    let count = packet.length()?;
    if count > 128 {
        return Err(Error::new(1, "plugin credential field capacity exceeded"));
    }
    let mut fields = Vec::with_capacity(count);
    let mut size = 0;
    for _ in 0..count {
        let component = match packet.take(1)?[0] {
            0 => ComponentSlot::Source,
            1 => ComponentSlot::Bootstrap,
            _ => {
                return Err(Error::new(
                    1,
                    "plugin credential packet contains an unknown component",
                ))
            }
        };
        let field = packet.text(256)?;
        let reference = SecretRef {
            provider: packet.text(128)?,
            key: packet.text(1024)?,
            version: packet.text(1024)?,
        };
        let value = packet.blob(16 << 10)?;
        size += value.len();
        if size > 256 << 10 {
            return Err(Error::new(
                1,
                "plugin credential group byte capacity exceeded",
            ));
        }
        fields.push(ResolvedField::new(
            component,
            field,
            reference,
            value.to_vec(),
        ));
    }
    if !packet.0.is_empty() {
        return Err(Error::new(
            1,
            "plugin credential packet contains trailing bytes",
        ));
    }
    AdmissionValues::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(value: &[u8]) -> Vec<u8> {
        let mut bytes = b"TDC1\x01\x00\x00\x00\x00".to_vec();
        for field in [b"f".as_slice(), b"p", b"k", b"v", value] {
            bytes.extend_from_slice(&(field.len() as u32).to_le_bytes());
            bytes.extend_from_slice(field);
        }
        bytes
    }

    #[test]
    fn golden_packet_and_every_truncated_prefix() {
        let bytes = b"TDC1\x01\x00\x00\x00\x00\
            \x01\x00\x00\x00f\x01\x00\x00\x00p\
            \x01\x00\x00\x00k\x01\x00\x00\x00v\
            \x11\x00\x00\x00opaque-test-value";
        assert!(decode(bytes).is_ok());
        assert_eq!(
            format!("{:?}", decode(bytes).unwrap()),
            "AdmissionValues(<redacted>)"
        );
        for length in 0..bytes.len() {
            assert!(
                decode(&bytes[..length]).is_err(),
                "accepted prefix {length}"
            );
        }
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
    }

    #[test]
    fn invalid_packets_are_bounded_and_errors_do_not_echo_values() {
        let mut duplicate = packet(b"opaque-test-value");
        duplicate[4..8].copy_from_slice(&2_u32.to_le_bytes());
        duplicate.extend_from_slice(&packet(b"opaque-test-value")[8..]);
        let mut unknown_slot = packet(b"opaque-test-value");
        unknown_slot[8] = 2;
        let mut count = b"TDC1".to_vec();
        count.extend_from_slice(&129_u32.to_le_bytes());
        for bytes in [
            packet(b"opaque-test-value\0"),
            packet(&[0xff]),
            packet(&vec![b'x'; (16 << 10) + 1]),
            vec![0; MAX_INPUT + 1],
            duplicate,
            unknown_slot,
            count,
        ] {
            let error = decode(&bytes).unwrap_err();
            assert_eq!(error.code, 1);
            assert!(!error.message.contains("opaque-test-value"));
        }
    }
}
