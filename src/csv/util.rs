use stable_eyre::eyre::Result;

pub struct ReadItem<T: Sized> {
    pub raw: Option<String>,
    pub result: Result<T>,
}
