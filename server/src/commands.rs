use anyhow::Result;

pub(crate) fn process_request(request: &str) -> Result<String> {
    Ok(request.to_owned())
}
