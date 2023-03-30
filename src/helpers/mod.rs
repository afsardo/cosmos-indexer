use base64::{engine::general_purpose, Engine as _};
use serde::{self, Deserialize, Deserializer};

pub fn deserialize_string_to_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;

    s.parse().map_err(serde::de::Error::custom)
}

pub fn deserialize_base64_to_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;

    if let Some(s) = s {
        let base64_decoded = general_purpose::STANDARD.decode(&s);

        if base64_decoded.is_err() {
            return Ok(None);
        }

        let string_encoded = String::from_utf8(base64_decoded.unwrap());

        if string_encoded.is_err() {
            return Ok(None);
        }

        return Ok(Some(string_encoded.unwrap()));
    }

    Ok(None)
}
