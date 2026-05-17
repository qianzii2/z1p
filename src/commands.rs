pub enum Command {
    Open(OpenOptions),
    Use(UseOptions),
    Close(String),
    CloseUse,
    List,
    Schema(String),
    Sql(String),
    Exit,
    Export(ExportOptions),
}

pub struct UseOptions {
    pub path: String,
    pub table_name: String,
}

pub struct OpenOptions {
    pub path: String,
    pub table_name: String,
}

pub struct ExportOptions {
    pub path: String,
    pub compression: String,
}
