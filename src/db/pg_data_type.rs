use crate::db::data_type::DataType;
use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef};
use sqlx::{Decode, Encode, Postgres, Type};

impl Type<Postgres> for DataType {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("sync_data_type")
    }
}

impl<'r> Decode<'r, Postgres> for DataType {
    fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as Decode<Postgres>>::decode(value)?;
        Ok(DataType(s))
    }
}

impl Encode<'_, Postgres> for DataType {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> sqlx::encode::IsNull {
        <String as Encode<Postgres>>::encode_by_ref(&self.0, buf)
    }
}
