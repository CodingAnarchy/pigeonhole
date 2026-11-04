impl serde::Serialize for AggregateInput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.family.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        if self.timestamp.is_some() {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.AggregateInput", len)?;
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        if let Some(v) = self.timestamp.as_ref() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("timestamp", ToString::to_string(&v).as_str())?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggregateInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "family",
            "qualifier",
            "timestamp",
            "input",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Family,
            Qualifier,
            Timestamp,
            Input,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "family" => Ok(GeneratedField::Family),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            "input" => Ok(GeneratedField::Input),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.AggregateInput")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AggregateInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut family__ = None;
                let mut qualifier__ = None;
                let mut timestamp__ = None;
                let mut input__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AggregateInput {
                    family: family__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                    timestamp: timestamp__,
                    input: input__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.AggregateInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateQuery {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.quantiles.is_empty() {
            len += 1;
        }
        if !self.bloom_i64.is_empty() {
            len += 1;
        }
        if !self.bloom_bytes.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.AggregateQuery", len)?;
        if !self.quantiles.is_empty() {
            struct_ser.serialize_field("quantiles", &self.quantiles)?;
        }
        if !self.bloom_i64.is_empty() {
            struct_ser.serialize_field("bloom_i64", &self.bloom_i64.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        if !self.bloom_bytes.is_empty() {
            struct_ser.serialize_field("bloom_bytes", &self.bloom_bytes.iter().map(pbjson::private::base64::encode).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggregateQuery {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "quantiles",
            "bloom_i64",
            "bloomI64",
            "bloom_bytes",
            "bloomBytes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Quantiles,
            BloomI64,
            BloomBytes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "quantiles" => Ok(GeneratedField::Quantiles),
                            "bloomI64" | "bloom_i64" => Ok(GeneratedField::BloomI64),
                            "bloomBytes" | "bloom_bytes" => Ok(GeneratedField::BloomBytes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateQuery;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.AggregateQuery")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AggregateQuery, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut quantiles__ = None;
                let mut bloom_i64__ = None;
                let mut bloom_bytes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Quantiles => {
                            if quantiles__.is_some() {
                                return Err(serde::de::Error::duplicate_field("quantiles"));
                            }
                            quantiles__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::BloomI64 => {
                            if bloom_i64__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomI64"));
                            }
                            bloom_i64__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::BloomBytes => {
                            if bloom_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomBytes"));
                            }
                            bloom_bytes__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(AggregateQuery {
                    quantiles: quantiles__.unwrap_or_default(),
                    bloom_i64: bloom_i64__.unwrap_or_default(),
                    bloom_bytes: bloom_bytes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.AggregateQuery", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateView {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.hll_estimate.is_some() {
            len += 1;
        }
        if self.mean_var.is_some() {
            len += 1;
        }
        if !self.histogram_counts.is_empty() {
            len += 1;
        }
        if self.quantiles.is_some() {
            len += 1;
        }
        if self.bloom.is_some() {
            len += 1;
        }
        if !self.set_members.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.AggregateView", len)?;
        if let Some(v) = self.hll_estimate.as_ref() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("hll_estimate", ToString::to_string(&v).as_str())?;
        }
        if let Some(v) = self.mean_var.as_ref() {
            struct_ser.serialize_field("mean_var", v)?;
        }
        if !self.histogram_counts.is_empty() {
            struct_ser.serialize_field("histogram_counts", &self.histogram_counts.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        if let Some(v) = self.quantiles.as_ref() {
            struct_ser.serialize_field("quantiles", v)?;
        }
        if let Some(v) = self.bloom.as_ref() {
            struct_ser.serialize_field("bloom", v)?;
        }
        if !self.set_members.is_empty() {
            struct_ser.serialize_field("set_members", &self.set_members)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggregateView {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hll_estimate",
            "hllEstimate",
            "mean_var",
            "meanVar",
            "histogram_counts",
            "histogramCounts",
            "quantiles",
            "bloom",
            "set_members",
            "setMembers",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            HllEstimate,
            MeanVar,
            HistogramCounts,
            Quantiles,
            Bloom,
            SetMembers,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "hllEstimate" | "hll_estimate" => Ok(GeneratedField::HllEstimate),
                            "meanVar" | "mean_var" => Ok(GeneratedField::MeanVar),
                            "histogramCounts" | "histogram_counts" => Ok(GeneratedField::HistogramCounts),
                            "quantiles" => Ok(GeneratedField::Quantiles),
                            "bloom" => Ok(GeneratedField::Bloom),
                            "setMembers" | "set_members" => Ok(GeneratedField::SetMembers),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateView;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.AggregateView")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AggregateView, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hll_estimate__ = None;
                let mut mean_var__ = None;
                let mut histogram_counts__ = None;
                let mut quantiles__ = None;
                let mut bloom__ = None;
                let mut set_members__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::HllEstimate => {
                            if hll_estimate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hllEstimate"));
                            }
                            hll_estimate__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::MeanVar => {
                            if mean_var__.is_some() {
                                return Err(serde::de::Error::duplicate_field("meanVar"));
                            }
                            mean_var__ = map_.next_value()?;
                        }
                        GeneratedField::HistogramCounts => {
                            if histogram_counts__.is_some() {
                                return Err(serde::de::Error::duplicate_field("histogramCounts"));
                            }
                            histogram_counts__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::Quantiles => {
                            if quantiles__.is_some() {
                                return Err(serde::de::Error::duplicate_field("quantiles"));
                            }
                            quantiles__ = map_.next_value()?;
                        }
                        GeneratedField::Bloom => {
                            if bloom__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloom"));
                            }
                            bloom__ = map_.next_value()?;
                        }
                        GeneratedField::SetMembers => {
                            if set_members__.is_some() {
                                return Err(serde::de::Error::duplicate_field("setMembers"));
                            }
                            set_members__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(AggregateView {
                    hll_estimate: hll_estimate__,
                    mean_var: mean_var__,
                    histogram_counts: histogram_counts__.unwrap_or_default(),
                    quantiles: quantiles__,
                    bloom: bloom__,
                    set_members: set_members__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.AggregateView", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Aggregation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.kind != 0 {
            len += 1;
        }
        if !self.histogram_bounds.is_empty() {
            len += 1;
        }
        if self.quantile_mantissa_bits.is_some() {
            len += 1;
        }
        if self.quantile_max_buckets.is_some() {
            len += 1;
        }
        if self.bloom_bits != 0 {
            len += 1;
        }
        if self.bloom_hashes != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Aggregation", len)?;
        if self.kind != 0 {
            let v = AggregationKind::try_from(self.kind)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.kind)))?;
            struct_ser.serialize_field("kind", &v)?;
        }
        if !self.histogram_bounds.is_empty() {
            struct_ser.serialize_field("histogram_bounds", &self.histogram_bounds)?;
        }
        if let Some(v) = self.quantile_mantissa_bits.as_ref() {
            struct_ser.serialize_field("quantile_mantissa_bits", v)?;
        }
        if let Some(v) = self.quantile_max_buckets.as_ref() {
            struct_ser.serialize_field("quantile_max_buckets", v)?;
        }
        if self.bloom_bits != 0 {
            struct_ser.serialize_field("bloom_bits", &self.bloom_bits)?;
        }
        if self.bloom_hashes != 0 {
            struct_ser.serialize_field("bloom_hashes", &self.bloom_hashes)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Aggregation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "kind",
            "histogram_bounds",
            "histogramBounds",
            "quantile_mantissa_bits",
            "quantileMantissaBits",
            "quantile_max_buckets",
            "quantileMaxBuckets",
            "bloom_bits",
            "bloomBits",
            "bloom_hashes",
            "bloomHashes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Kind,
            HistogramBounds,
            QuantileMantissaBits,
            QuantileMaxBuckets,
            BloomBits,
            BloomHashes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "kind" => Ok(GeneratedField::Kind),
                            "histogramBounds" | "histogram_bounds" => Ok(GeneratedField::HistogramBounds),
                            "quantileMantissaBits" | "quantile_mantissa_bits" => Ok(GeneratedField::QuantileMantissaBits),
                            "quantileMaxBuckets" | "quantile_max_buckets" => Ok(GeneratedField::QuantileMaxBuckets),
                            "bloomBits" | "bloom_bits" => Ok(GeneratedField::BloomBits),
                            "bloomHashes" | "bloom_hashes" => Ok(GeneratedField::BloomHashes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Aggregation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Aggregation")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Aggregation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut kind__ = None;
                let mut histogram_bounds__ = None;
                let mut quantile_mantissa_bits__ = None;
                let mut quantile_max_buckets__ = None;
                let mut bloom_bits__ = None;
                let mut bloom_hashes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Kind => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("kind"));
                            }
                            kind__ = Some(map_.next_value::<AggregationKind>()? as i32);
                        }
                        GeneratedField::HistogramBounds => {
                            if histogram_bounds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("histogramBounds"));
                            }
                            histogram_bounds__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::QuantileMantissaBits => {
                            if quantile_mantissa_bits__.is_some() {
                                return Err(serde::de::Error::duplicate_field("quantileMantissaBits"));
                            }
                            quantile_mantissa_bits__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::QuantileMaxBuckets => {
                            if quantile_max_buckets__.is_some() {
                                return Err(serde::de::Error::duplicate_field("quantileMaxBuckets"));
                            }
                            quantile_max_buckets__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::BloomBits => {
                            if bloom_bits__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomBits"));
                            }
                            bloom_bits__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::BloomHashes => {
                            if bloom_hashes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomHashes"));
                            }
                            bloom_hashes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Aggregation {
                    kind: kind__.unwrap_or_default(),
                    histogram_bounds: histogram_bounds__.unwrap_or_default(),
                    quantile_mantissa_bits: quantile_mantissa_bits__,
                    quantile_max_buckets: quantile_max_buckets__,
                    bloom_bits: bloom_bits__.unwrap_or_default(),
                    bloom_hashes: bloom_hashes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Aggregation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AggregationKind {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "AGGREGATION_KIND_UNSPECIFIED",
            Self::Sum => "AGGREGATION_KIND_SUM",
            Self::Min => "AGGREGATION_KIND_MIN",
            Self::Max => "AGGREGATION_KIND_MAX",
            Self::Hll => "AGGREGATION_KIND_HLL",
            Self::BitOr => "AGGREGATION_KIND_BIT_OR",
            Self::BitAnd => "AGGREGATION_KIND_BIT_AND",
            Self::BitXor => "AGGREGATION_KIND_BIT_XOR",
            Self::MinF64 => "AGGREGATION_KIND_MIN_F64",
            Self::MaxF64 => "AGGREGATION_KIND_MAX_F64",
            Self::SumF64 => "AGGREGATION_KIND_SUM_F64",
            Self::MeanVar => "AGGREGATION_KIND_MEAN_VAR",
            Self::Histogram => "AGGREGATION_KIND_HISTOGRAM",
            Self::Quantiles => "AGGREGATION_KIND_QUANTILES",
            Self::Bloom => "AGGREGATION_KIND_BLOOM",
            Self::Set => "AGGREGATION_KIND_SET",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for AggregationKind {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "AGGREGATION_KIND_UNSPECIFIED",
            "AGGREGATION_KIND_SUM",
            "AGGREGATION_KIND_MIN",
            "AGGREGATION_KIND_MAX",
            "AGGREGATION_KIND_HLL",
            "AGGREGATION_KIND_BIT_OR",
            "AGGREGATION_KIND_BIT_AND",
            "AGGREGATION_KIND_BIT_XOR",
            "AGGREGATION_KIND_MIN_F64",
            "AGGREGATION_KIND_MAX_F64",
            "AGGREGATION_KIND_SUM_F64",
            "AGGREGATION_KIND_MEAN_VAR",
            "AGGREGATION_KIND_HISTOGRAM",
            "AGGREGATION_KIND_QUANTILES",
            "AGGREGATION_KIND_BLOOM",
            "AGGREGATION_KIND_SET",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = AggregationKind;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "AGGREGATION_KIND_UNSPECIFIED" => Ok(AggregationKind::Unspecified),
                    "AGGREGATION_KIND_SUM" => Ok(AggregationKind::Sum),
                    "AGGREGATION_KIND_MIN" => Ok(AggregationKind::Min),
                    "AGGREGATION_KIND_MAX" => Ok(AggregationKind::Max),
                    "AGGREGATION_KIND_HLL" => Ok(AggregationKind::Hll),
                    "AGGREGATION_KIND_BIT_OR" => Ok(AggregationKind::BitOr),
                    "AGGREGATION_KIND_BIT_AND" => Ok(AggregationKind::BitAnd),
                    "AGGREGATION_KIND_BIT_XOR" => Ok(AggregationKind::BitXor),
                    "AGGREGATION_KIND_MIN_F64" => Ok(AggregationKind::MinF64),
                    "AGGREGATION_KIND_MAX_F64" => Ok(AggregationKind::MaxF64),
                    "AGGREGATION_KIND_SUM_F64" => Ok(AggregationKind::SumF64),
                    "AGGREGATION_KIND_MEAN_VAR" => Ok(AggregationKind::MeanVar),
                    "AGGREGATION_KIND_HISTOGRAM" => Ok(AggregationKind::Histogram),
                    "AGGREGATION_KIND_QUANTILES" => Ok(AggregationKind::Quantiles),
                    "AGGREGATION_KIND_BLOOM" => Ok(AggregationKind::Bloom),
                    "AGGREGATION_KIND_SET" => Ok(AggregationKind::Set),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for BackupChunk {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.data.is_empty() {
            len += 1;
        }
        if self.total != 0 {
            len += 1;
        }
        if !self.backup_id.is_empty() {
            len += 1;
        }
        if self.offset != 0 {
            len += 1;
        }
        if !self.sha256.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.BackupChunk", len)?;
        if !self.data.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("data", pbjson::private::base64::encode(&self.data).as_str())?;
        }
        if self.total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("total", ToString::to_string(&self.total).as_str())?;
        }
        if !self.backup_id.is_empty() {
            struct_ser.serialize_field("backup_id", &self.backup_id)?;
        }
        if self.offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("offset", ToString::to_string(&self.offset).as_str())?;
        }
        if !self.sha256.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("sha256", pbjson::private::base64::encode(&self.sha256).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BackupChunk {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "data",
            "total",
            "backup_id",
            "backupId",
            "offset",
            "sha256",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Data,
            Total,
            BackupId,
            Offset,
            Sha256,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "data" => Ok(GeneratedField::Data),
                            "total" => Ok(GeneratedField::Total),
                            "backupId" | "backup_id" => Ok(GeneratedField::BackupId),
                            "offset" => Ok(GeneratedField::Offset),
                            "sha256" => Ok(GeneratedField::Sha256),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BackupChunk;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.BackupChunk")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BackupChunk, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut data__ = None;
                let mut total__ = None;
                let mut backup_id__ = None;
                let mut offset__ = None;
                let mut sha256__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Total => {
                            if total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("total"));
                            }
                            total__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::BackupId => {
                            if backup_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("backupId"));
                            }
                            backup_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Offset => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Sha256 => {
                            if sha256__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sha256"));
                            }
                            sha256__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(BackupChunk {
                    data: data__.unwrap_or_default(),
                    total: total__.unwrap_or_default(),
                    backup_id: backup_id__.unwrap_or_default(),
                    offset: offset__.unwrap_or_default(),
                    sha256: sha256__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.BackupChunk", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BackupDownloadRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.durable {
            len += 1;
        }
        if !self.backup_id.is_empty() {
            len += 1;
        }
        if self.offset != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.BackupDownloadRequest", len)?;
        if self.durable {
            struct_ser.serialize_field("durable", &self.durable)?;
        }
        if !self.backup_id.is_empty() {
            struct_ser.serialize_field("backup_id", &self.backup_id)?;
        }
        if self.offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("offset", ToString::to_string(&self.offset).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BackupDownloadRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "durable",
            "backup_id",
            "backupId",
            "offset",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Durable,
            BackupId,
            Offset,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "durable" => Ok(GeneratedField::Durable),
                            "backupId" | "backup_id" => Ok(GeneratedField::BackupId),
                            "offset" => Ok(GeneratedField::Offset),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BackupDownloadRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.BackupDownloadRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BackupDownloadRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut durable__ = None;
                let mut backup_id__ = None;
                let mut offset__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Durable => {
                            if durable__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durable"));
                            }
                            durable__ = Some(map_.next_value()?);
                        }
                        GeneratedField::BackupId => {
                            if backup_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("backupId"));
                            }
                            backup_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Offset => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(BackupDownloadRequest {
                    durable: durable__.unwrap_or_default(),
                    backup_id: backup_id__.unwrap_or_default(),
                    offset: offset__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.BackupDownloadRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BackupRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.path.is_empty() {
            len += 1;
        }
        if self.durable {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.BackupRequest", len)?;
        if !self.path.is_empty() {
            struct_ser.serialize_field("path", &self.path)?;
        }
        if self.durable {
            struct_ser.serialize_field("durable", &self.durable)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BackupRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "path",
            "durable",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Path,
            Durable,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "path" => Ok(GeneratedField::Path),
                            "durable" => Ok(GeneratedField::Durable),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BackupRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.BackupRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BackupRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut path__ = None;
                let mut durable__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Path => {
                            if path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("path"));
                            }
                            path__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Durable => {
                            if durable__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durable"));
                            }
                            durable__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(BackupRequest {
                    path: path__.unwrap_or_default(),
                    durable: durable__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.BackupRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BackupResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.bytes != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.BackupResponse", len)?;
        if self.bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("bytes", ToString::to_string(&self.bytes).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BackupResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "bytes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Bytes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "bytes" => Ok(GeneratedField::Bytes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BackupResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.BackupResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BackupResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut bytes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Bytes => {
                            if bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bytes"));
                            }
                            bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(BackupResponse {
                    bytes: bytes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.BackupResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BatchRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.rows.is_empty() {
            len += 1;
        }
        if self.durability != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.BatchRequest", len)?;
        if !self.rows.is_empty() {
            struct_ser.serialize_field("rows", &self.rows)?;
        }
        if self.durability != 0 {
            let v = Durability::try_from(self.durability)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.durability)))?;
            struct_ser.serialize_field("durability", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BatchRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "rows",
            "durability",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Rows,
            Durability,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "rows" => Ok(GeneratedField::Rows),
                            "durability" => Ok(GeneratedField::Durability),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BatchRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.BatchRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BatchRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut rows__ = None;
                let mut durability__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Rows => {
                            if rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rows"));
                            }
                            rows__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Durability => {
                            if durability__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durability"));
                            }
                            durability__ = Some(map_.next_value::<Durability>()? as i32);
                        }
                    }
                }
                Ok(BatchRequest {
                    rows: rows__.unwrap_or_default(),
                    durability: durability__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.BatchRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BeginRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.durability != 0 {
            len += 1;
        }
        if self.route.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.BeginRequest", len)?;
        if self.durability != 0 {
            let v = Durability::try_from(self.durability)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.durability)))?;
            struct_ser.serialize_field("durability", &v)?;
        }
        if let Some(v) = self.route.as_ref() {
            struct_ser.serialize_field("route", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BeginRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "durability",
            "route",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Durability,
            Route,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "durability" => Ok(GeneratedField::Durability),
                            "route" => Ok(GeneratedField::Route),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BeginRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.BeginRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BeginRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut durability__ = None;
                let mut route__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Durability => {
                            if durability__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durability"));
                            }
                            durability__ = Some(map_.next_value::<Durability>()? as i32);
                        }
                        GeneratedField::Route => {
                            if route__.is_some() {
                                return Err(serde::de::Error::duplicate_field("route"));
                            }
                            route__ = map_.next_value()?;
                        }
                    }
                }
                Ok(BeginRequest {
                    durability: durability__.unwrap_or_default(),
                    route: route__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.BeginRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BeginResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.txn_id.is_empty() {
            len += 1;
        }
        if self.snapshot_seqno != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.BeginResponse", len)?;
        if !self.txn_id.is_empty() {
            struct_ser.serialize_field("txn_id", &self.txn_id)?;
        }
        if self.snapshot_seqno != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("snapshot_seqno", ToString::to_string(&self.snapshot_seqno).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BeginResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "txn_id",
            "txnId",
            "snapshot_seqno",
            "snapshotSeqno",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TxnId,
            SnapshotSeqno,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "txnId" | "txn_id" => Ok(GeneratedField::TxnId),
                            "snapshotSeqno" | "snapshot_seqno" => Ok(GeneratedField::SnapshotSeqno),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BeginResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.BeginResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BeginResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut txn_id__ = None;
                let mut snapshot_seqno__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TxnId => {
                            if txn_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("txnId"));
                            }
                            txn_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SnapshotSeqno => {
                            if snapshot_seqno__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotSeqno"));
                            }
                            snapshot_seqno__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(BeginResponse {
                    txn_id: txn_id__.unwrap_or_default(),
                    snapshot_seqno: snapshot_seqno__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.BeginResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BloomReading {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.fill != 0. {
            len += 1;
        }
        if !self.contains.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.BloomReading", len)?;
        if self.fill != 0. {
            struct_ser.serialize_field("fill", &self.fill)?;
        }
        if !self.contains.is_empty() {
            struct_ser.serialize_field("contains", &self.contains)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BloomReading {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fill",
            "contains",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Fill,
            Contains,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "fill" => Ok(GeneratedField::Fill),
                            "contains" => Ok(GeneratedField::Contains),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BloomReading;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.BloomReading")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BloomReading, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fill__ = None;
                let mut contains__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Fill => {
                            if fill__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fill"));
                            }
                            fill__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Contains => {
                            if contains__.is_some() {
                                return Err(serde::de::Error::duplicate_field("contains"));
                            }
                            contains__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(BloomReading {
                    fill: fill__.unwrap_or_default(),
                    contains: contains__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.BloomReading", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Bound {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.key.is_empty() {
            len += 1;
        }
        if self.inclusive {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Bound", len)?;
        if !self.key.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("key", pbjson::private::base64::encode(&self.key).as_str())?;
        }
        if self.inclusive {
            struct_ser.serialize_field("inclusive", &self.inclusive)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Bound {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "key",
            "inclusive",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Key,
            Inclusive,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "key" => Ok(GeneratedField::Key),
                            "inclusive" => Ok(GeneratedField::Inclusive),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Bound;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Bound")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Bound, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut key__ = None;
                let mut inclusive__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Key => {
                            if key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Inclusive => {
                            if inclusive__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inclusive"));
                            }
                            inclusive__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Bound {
                    key: key__.unwrap_or_default(),
                    inclusive: inclusive__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Bound", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Bounds {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start.is_some() {
            len += 1;
        }
        if self.end.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Bounds", len)?;
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if let Some(v) = self.end.as_ref() {
            struct_ser.serialize_field("end", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Bounds {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Bounds;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Bounds")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Bounds, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = map_.next_value()?;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Bounds {
                    start: start__,
                    end: end__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Bounds", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BuiltinLink {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "BUILTIN_LINK_UNSPECIFIED",
            Self::DerivedFrom => "BUILTIN_LINK_DERIVED_FROM",
            Self::SimilarTo => "BUILTIN_LINK_SIMILAR_TO",
            Self::Supersedes => "BUILTIN_LINK_SUPERSEDES",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for BuiltinLink {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "BUILTIN_LINK_UNSPECIFIED",
            "BUILTIN_LINK_DERIVED_FROM",
            "BUILTIN_LINK_SIMILAR_TO",
            "BUILTIN_LINK_SUPERSEDES",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = BuiltinLink;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "BUILTIN_LINK_UNSPECIFIED" => Ok(BuiltinLink::Unspecified),
                    "BUILTIN_LINK_DERIVED_FROM" => Ok(BuiltinLink::DerivedFrom),
                    "BUILTIN_LINK_SIMILAR_TO" => Ok(BuiltinLink::SimilarTo),
                    "BUILTIN_LINK_SUPERSEDES" => Ok(BuiltinLink::Supersedes),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Cell {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.family.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        if self.timestamp != 0 {
            len += 1;
        }
        if !self.value.is_empty() {
            len += 1;
        }
        if self.typed.is_some() {
            len += 1;
        }
        if self.aggregate.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Cell", len)?;
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        if self.timestamp != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("timestamp", ToString::to_string(&self.timestamp).as_str())?;
        }
        if !self.value.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("value", pbjson::private::base64::encode(&self.value).as_str())?;
        }
        if let Some(v) = self.typed.as_ref() {
            struct_ser.serialize_field("typed", v)?;
        }
        if let Some(v) = self.aggregate.as_ref() {
            struct_ser.serialize_field("aggregate", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Cell {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "family",
            "qualifier",
            "timestamp",
            "value",
            "typed",
            "aggregate",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Family,
            Qualifier,
            Timestamp,
            Value,
            Typed,
            Aggregate,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "family" => Ok(GeneratedField::Family),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            "value" => Ok(GeneratedField::Value),
                            "typed" => Ok(GeneratedField::Typed),
                            "aggregate" => Ok(GeneratedField::Aggregate),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Cell;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Cell")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Cell, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut family__ = None;
                let mut qualifier__ = None;
                let mut timestamp__ = None;
                let mut value__ = None;
                let mut typed__ = None;
                let mut aggregate__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Typed => {
                            if typed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("typed"));
                            }
                            typed__ = map_.next_value()?;
                        }
                        GeneratedField::Aggregate => {
                            if aggregate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregate"));
                            }
                            aggregate__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Cell {
                    family: family__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                    timestamp: timestamp__.unwrap_or_default(),
                    value: value__.unwrap_or_default(),
                    typed: typed__,
                    aggregate: aggregate__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Cell", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Change {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table.is_empty() {
            len += 1;
        }
        if !self.family.is_empty() {
            len += 1;
        }
        if self.kind != 0 {
            len += 1;
        }
        if !self.row.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        if self.timestamp != 0 {
            len += 1;
        }
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Change", len)?;
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if self.kind != 0 {
            let v = ChangeKind::try_from(self.kind)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.kind)))?;
            struct_ser.serialize_field("kind", &v)?;
        }
        if !self.row.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("row", pbjson::private::base64::encode(&self.row).as_str())?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        if self.timestamp != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("timestamp", ToString::to_string(&self.timestamp).as_str())?;
        }
        if let Some(v) = self.value.as_ref() {
            struct_ser.serialize_field("value", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Change {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table",
            "family",
            "kind",
            "row",
            "qualifier",
            "timestamp",
            "value",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Table,
            Family,
            Kind,
            Row,
            Qualifier,
            Timestamp,
            Value,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "table" => Ok(GeneratedField::Table),
                            "family" => Ok(GeneratedField::Family),
                            "kind" => Ok(GeneratedField::Kind),
                            "row" => Ok(GeneratedField::Row),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            "value" => Ok(GeneratedField::Value),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Change;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Change")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Change, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table__ = None;
                let mut family__ = None;
                let mut kind__ = None;
                let mut row__ = None;
                let mut qualifier__ = None;
                let mut timestamp__ = None;
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Kind => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("kind"));
                            }
                            kind__ = Some(map_.next_value::<ChangeKind>()? as i32);
                        }
                        GeneratedField::Row => {
                            if row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("row"));
                            }
                            row__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Change {
                    table: table__.unwrap_or_default(),
                    family: family__.unwrap_or_default(),
                    kind: kind__.unwrap_or_default(),
                    row: row__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                    timestamp: timestamp__.unwrap_or_default(),
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Change", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ChangeKind {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "CHANGE_KIND_UNSPECIFIED",
            Self::Put => "CHANGE_KIND_PUT",
            Self::Merge => "CHANGE_KIND_MERGE",
            Self::DeleteCell => "CHANGE_KIND_DELETE_CELL",
            Self::DeleteColumn => "CHANGE_KIND_DELETE_COLUMN",
            Self::DeleteFamily => "CHANGE_KIND_DELETE_FAMILY",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for ChangeKind {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "CHANGE_KIND_UNSPECIFIED",
            "CHANGE_KIND_PUT",
            "CHANGE_KIND_MERGE",
            "CHANGE_KIND_DELETE_CELL",
            "CHANGE_KIND_DELETE_COLUMN",
            "CHANGE_KIND_DELETE_FAMILY",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = ChangeKind;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "CHANGE_KIND_UNSPECIFIED" => Ok(ChangeKind::Unspecified),
                    "CHANGE_KIND_PUT" => Ok(ChangeKind::Put),
                    "CHANGE_KIND_MERGE" => Ok(ChangeKind::Merge),
                    "CHANGE_KIND_DELETE_CELL" => Ok(ChangeKind::DeleteCell),
                    "CHANGE_KIND_DELETE_COLUMN" => Ok(ChangeKind::DeleteColumn),
                    "CHANGE_KIND_DELETE_FAMILY" => Ok(ChangeKind::DeleteFamily),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for CheckAndMutateRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.writes.is_some() {
            len += 1;
        }
        if self.condition.is_some() {
            len += 1;
        }
        if self.durability != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.CheckAndMutateRequest", len)?;
        if let Some(v) = self.writes.as_ref() {
            struct_ser.serialize_field("writes", v)?;
        }
        if let Some(v) = self.condition.as_ref() {
            struct_ser.serialize_field("condition", v)?;
        }
        if self.durability != 0 {
            let v = Durability::try_from(self.durability)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.durability)))?;
            struct_ser.serialize_field("durability", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CheckAndMutateRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "writes",
            "condition",
            "durability",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Writes,
            Condition,
            Durability,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "writes" => Ok(GeneratedField::Writes),
                            "condition" => Ok(GeneratedField::Condition),
                            "durability" => Ok(GeneratedField::Durability),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CheckAndMutateRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.CheckAndMutateRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CheckAndMutateRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut writes__ = None;
                let mut condition__ = None;
                let mut durability__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Writes => {
                            if writes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("writes"));
                            }
                            writes__ = map_.next_value()?;
                        }
                        GeneratedField::Condition => {
                            if condition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("condition"));
                            }
                            condition__ = map_.next_value()?;
                        }
                        GeneratedField::Durability => {
                            if durability__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durability"));
                            }
                            durability__ = Some(map_.next_value::<Durability>()? as i32);
                        }
                    }
                }
                Ok(CheckAndMutateRequest {
                    writes: writes__,
                    condition: condition__,
                    durability: durability__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.CheckAndMutateRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CheckAndMutateResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.applied {
            len += 1;
        }
        if self.commit.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.CheckAndMutateResponse", len)?;
        if self.applied {
            struct_ser.serialize_field("applied", &self.applied)?;
        }
        if let Some(v) = self.commit.as_ref() {
            struct_ser.serialize_field("commit", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CheckAndMutateResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "applied",
            "commit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Applied,
            Commit,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "applied" => Ok(GeneratedField::Applied),
                            "commit" => Ok(GeneratedField::Commit),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CheckAndMutateResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.CheckAndMutateResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CheckAndMutateResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut applied__ = None;
                let mut commit__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Applied => {
                            if applied__.is_some() {
                                return Err(serde::de::Error::duplicate_field("applied"));
                            }
                            applied__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Commit => {
                            if commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("commit"));
                            }
                            commit__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CheckAndMutateResponse {
                    applied: applied__.unwrap_or_default(),
                    commit: commit__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.CheckAndMutateResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Column {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.family.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Column", len)?;
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Column {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "family",
            "qualifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Family,
            Qualifier,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "family" => Ok(GeneratedField::Family),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Column;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Column")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Column, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut family__ = None;
                let mut qualifier__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Column {
                    family: family__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Column", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CommitInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.seqno != 0 {
            len += 1;
        }
        if self.durability != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.CommitInfo", len)?;
        if self.seqno != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("seqno", ToString::to_string(&self.seqno).as_str())?;
        }
        if self.durability != 0 {
            let v = Durability::try_from(self.durability)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.durability)))?;
            struct_ser.serialize_field("durability", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CommitInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "seqno",
            "durability",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Seqno,
            Durability,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "seqno" => Ok(GeneratedField::Seqno),
                            "durability" => Ok(GeneratedField::Durability),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CommitInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.CommitInfo")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CommitInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut seqno__ = None;
                let mut durability__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Seqno => {
                            if seqno__.is_some() {
                                return Err(serde::de::Error::duplicate_field("seqno"));
                            }
                            seqno__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Durability => {
                            if durability__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durability"));
                            }
                            durability__ = Some(map_.next_value::<Durability>()? as i32);
                        }
                    }
                }
                Ok(CommitInfo {
                    seqno: seqno__.unwrap_or_default(),
                    durability: durability__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.CommitInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CompactionStyle {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "COMPACTION_STYLE_UNSPECIFIED",
            Self::Leveled => "COMPACTION_STYLE_LEVELED",
            Self::Tiered => "COMPACTION_STYLE_TIERED",
            Self::FifoByTime => "COMPACTION_STYLE_FIFO_BY_TIME",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for CompactionStyle {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "COMPACTION_STYLE_UNSPECIFIED",
            "COMPACTION_STYLE_LEVELED",
            "COMPACTION_STYLE_TIERED",
            "COMPACTION_STYLE_FIFO_BY_TIME",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = CompactionStyle;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "COMPACTION_STYLE_UNSPECIFIED" => Ok(CompactionStyle::Unspecified),
                    "COMPACTION_STYLE_LEVELED" => Ok(CompactionStyle::Leveled),
                    "COMPACTION_STYLE_TIERED" => Ok(CompactionStyle::Tiered),
                    "COMPACTION_STYLE_FIFO_BY_TIME" => Ok(CompactionStyle::FifoByTime),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Compression {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.codec != 0 {
            len += 1;
        }
        if self.zstd_level != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Compression", len)?;
        if self.codec != 0 {
            let v = CompressionCodec::try_from(self.codec)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.codec)))?;
            struct_ser.serialize_field("codec", &v)?;
        }
        if self.zstd_level != 0 {
            struct_ser.serialize_field("zstd_level", &self.zstd_level)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Compression {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "codec",
            "zstd_level",
            "zstdLevel",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Codec,
            ZstdLevel,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "codec" => Ok(GeneratedField::Codec),
                            "zstdLevel" | "zstd_level" => Ok(GeneratedField::ZstdLevel),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Compression;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Compression")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Compression, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut codec__ = None;
                let mut zstd_level__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Codec => {
                            if codec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("codec"));
                            }
                            codec__ = Some(map_.next_value::<CompressionCodec>()? as i32);
                        }
                        GeneratedField::ZstdLevel => {
                            if zstd_level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("zstdLevel"));
                            }
                            zstd_level__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Compression {
                    codec: codec__.unwrap_or_default(),
                    zstd_level: zstd_level__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Compression", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CompressionCodec {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "COMPRESSION_CODEC_UNSPECIFIED",
            Self::None => "COMPRESSION_CODEC_NONE",
            Self::Lz4 => "COMPRESSION_CODEC_LZ4",
            Self::Zstd => "COMPRESSION_CODEC_ZSTD",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for CompressionCodec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "COMPRESSION_CODEC_UNSPECIFIED",
            "COMPRESSION_CODEC_NONE",
            "COMPRESSION_CODEC_LZ4",
            "COMPRESSION_CODEC_ZSTD",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = CompressionCodec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "COMPRESSION_CODEC_UNSPECIFIED" => Ok(CompressionCodec::Unspecified),
                    "COMPRESSION_CODEC_NONE" => Ok(CompressionCodec::None),
                    "COMPRESSION_CODEC_LZ4" => Ok(CompressionCodec::Lz4),
                    "COMPRESSION_CODEC_ZSTD" => Ok(CompressionCodec::Zstd),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Condition {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.kind.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Condition", len)?;
        if let Some(v) = self.kind.as_ref() {
            match v {
                condition::Kind::Exists(v) => {
                    struct_ser.serialize_field("exists", v)?;
                }
                condition::Kind::Absent(v) => {
                    struct_ser.serialize_field("absent", v)?;
                }
                condition::Kind::Value(v) => {
                    struct_ser.serialize_field("value", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Condition {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "exists",
            "absent",
            "value",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Exists,
            Absent,
            Value,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "exists" => Ok(GeneratedField::Exists),
                            "absent" => Ok(GeneratedField::Absent),
                            "value" => Ok(GeneratedField::Value),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Condition;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Condition")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Condition, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut kind__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Exists => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exists"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<_>>()?.map(condition::Kind::Exists)
;
                        }
                        GeneratedField::Absent => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("absent"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<_>>()?.map(condition::Kind::Absent)
;
                        }
                        GeneratedField::Value => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<_>>()?.map(condition::Kind::Value)
;
                        }
                    }
                }
                Ok(Condition {
                    kind: kind__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Condition", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateTableRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.families.is_empty() {
            len += 1;
        }
        if self.if_missing {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.CreateTableRequest", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.families.is_empty() {
            struct_ser.serialize_field("families", &self.families)?;
        }
        if self.if_missing {
            struct_ser.serialize_field("if_missing", &self.if_missing)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateTableRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "families",
            "if_missing",
            "ifMissing",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Families,
            IfMissing,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "families" => Ok(GeneratedField::Families),
                            "ifMissing" | "if_missing" => Ok(GeneratedField::IfMissing),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateTableRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.CreateTableRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CreateTableRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut families__ = None;
                let mut if_missing__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Families => {
                            if families__.is_some() {
                                return Err(serde::de::Error::duplicate_field("families"));
                            }
                            families__ = Some(map_.next_value()?);
                        }
                        GeneratedField::IfMissing => {
                            if if_missing__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ifMissing"));
                            }
                            if_missing__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CreateTableRequest {
                    name: name__.unwrap_or_default(),
                    families: families__.unwrap_or_default(),
                    if_missing: if_missing__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.CreateTableRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateTextIndexRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.source_table.is_empty() {
            len += 1;
        }
        if !self.source_family.is_empty() {
            len += 1;
        }
        if !self.source_qualifier.is_empty() {
            len += 1;
        }
        if self.k1.is_some() {
            len += 1;
        }
        if self.b.is_some() {
            len += 1;
        }
        if self.positions.is_some() {
            len += 1;
        }
        if self.stop_words.is_some() {
            len += 1;
        }
        if self.normalization != 0 {
            len += 1;
        }
        if self.max_terms.is_some() {
            len += 1;
        }
        if self.max_term_bytes.is_some() {
            len += 1;
        }
        if self.if_missing {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.CreateTextIndexRequest", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.source_table.is_empty() {
            struct_ser.serialize_field("source_table", &self.source_table)?;
        }
        if !self.source_family.is_empty() {
            struct_ser.serialize_field("source_family", &self.source_family)?;
        }
        if !self.source_qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("source_qualifier", pbjson::private::base64::encode(&self.source_qualifier).as_str())?;
        }
        if let Some(v) = self.k1.as_ref() {
            struct_ser.serialize_field("k1", v)?;
        }
        if let Some(v) = self.b.as_ref() {
            struct_ser.serialize_field("b", v)?;
        }
        if let Some(v) = self.positions.as_ref() {
            struct_ser.serialize_field("positions", v)?;
        }
        if let Some(v) = self.stop_words.as_ref() {
            struct_ser.serialize_field("stop_words", v)?;
        }
        if self.normalization != 0 {
            let v = Normalization::try_from(self.normalization)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.normalization)))?;
            struct_ser.serialize_field("normalization", &v)?;
        }
        if let Some(v) = self.max_terms.as_ref() {
            struct_ser.serialize_field("max_terms", v)?;
        }
        if let Some(v) = self.max_term_bytes.as_ref() {
            struct_ser.serialize_field("max_term_bytes", v)?;
        }
        if self.if_missing {
            struct_ser.serialize_field("if_missing", &self.if_missing)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateTextIndexRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "source_table",
            "sourceTable",
            "source_family",
            "sourceFamily",
            "source_qualifier",
            "sourceQualifier",
            "k1",
            "b",
            "positions",
            "stop_words",
            "stopWords",
            "normalization",
            "max_terms",
            "maxTerms",
            "max_term_bytes",
            "maxTermBytes",
            "if_missing",
            "ifMissing",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            SourceTable,
            SourceFamily,
            SourceQualifier,
            K1,
            B,
            Positions,
            StopWords,
            Normalization,
            MaxTerms,
            MaxTermBytes,
            IfMissing,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "sourceTable" | "source_table" => Ok(GeneratedField::SourceTable),
                            "sourceFamily" | "source_family" => Ok(GeneratedField::SourceFamily),
                            "sourceQualifier" | "source_qualifier" => Ok(GeneratedField::SourceQualifier),
                            "k1" => Ok(GeneratedField::K1),
                            "b" => Ok(GeneratedField::B),
                            "positions" => Ok(GeneratedField::Positions),
                            "stopWords" | "stop_words" => Ok(GeneratedField::StopWords),
                            "normalization" => Ok(GeneratedField::Normalization),
                            "maxTerms" | "max_terms" => Ok(GeneratedField::MaxTerms),
                            "maxTermBytes" | "max_term_bytes" => Ok(GeneratedField::MaxTermBytes),
                            "ifMissing" | "if_missing" => Ok(GeneratedField::IfMissing),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateTextIndexRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.CreateTextIndexRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CreateTextIndexRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut source_table__ = None;
                let mut source_family__ = None;
                let mut source_qualifier__ = None;
                let mut k1__ = None;
                let mut b__ = None;
                let mut positions__ = None;
                let mut stop_words__ = None;
                let mut normalization__ = None;
                let mut max_terms__ = None;
                let mut max_term_bytes__ = None;
                let mut if_missing__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SourceTable => {
                            if source_table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceTable"));
                            }
                            source_table__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SourceFamily => {
                            if source_family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceFamily"));
                            }
                            source_family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SourceQualifier => {
                            if source_qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceQualifier"));
                            }
                            source_qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::K1 => {
                            if k1__.is_some() {
                                return Err(serde::de::Error::duplicate_field("k1"));
                            }
                            k1__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::B => {
                            if b__.is_some() {
                                return Err(serde::de::Error::duplicate_field("b"));
                            }
                            b__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Positions => {
                            if positions__.is_some() {
                                return Err(serde::de::Error::duplicate_field("positions"));
                            }
                            positions__ = map_.next_value()?;
                        }
                        GeneratedField::StopWords => {
                            if stop_words__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stopWords"));
                            }
                            stop_words__ = map_.next_value()?;
                        }
                        GeneratedField::Normalization => {
                            if normalization__.is_some() {
                                return Err(serde::de::Error::duplicate_field("normalization"));
                            }
                            normalization__ = Some(map_.next_value::<Normalization>()? as i32);
                        }
                        GeneratedField::MaxTerms => {
                            if max_terms__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxTerms"));
                            }
                            max_terms__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::MaxTermBytes => {
                            if max_term_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxTermBytes"));
                            }
                            max_term_bytes__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::IfMissing => {
                            if if_missing__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ifMissing"));
                            }
                            if_missing__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CreateTextIndexRequest {
                    name: name__.unwrap_or_default(),
                    source_table: source_table__.unwrap_or_default(),
                    source_family: source_family__.unwrap_or_default(),
                    source_qualifier: source_qualifier__.unwrap_or_default(),
                    k1: k1__,
                    b: b__,
                    positions: positions__,
                    stop_words: stop_words__,
                    normalization: normalization__.unwrap_or_default(),
                    max_terms: max_terms__,
                    max_term_bytes: max_term_bytes__,
                    if_missing: if_missing__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.CreateTextIndexRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateVectorIndexRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.spec.is_some() {
            len += 1;
        }
        if self.target_list_len.is_some() {
            len += 1;
        }
        if self.nprobe.is_some() {
            len += 1;
        }
        if self.reassign_neighbours.is_some() {
            len += 1;
        }
        if self.rebalance != 0 {
            len += 1;
        }
        if self.if_missing {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.CreateVectorIndexRequest", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if let Some(v) = self.target_list_len.as_ref() {
            struct_ser.serialize_field("target_list_len", v)?;
        }
        if let Some(v) = self.nprobe.as_ref() {
            struct_ser.serialize_field("nprobe", v)?;
        }
        if let Some(v) = self.reassign_neighbours.as_ref() {
            struct_ser.serialize_field("reassign_neighbours", v)?;
        }
        if self.rebalance != 0 {
            let v = Rebalance::try_from(self.rebalance)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.rebalance)))?;
            struct_ser.serialize_field("rebalance", &v)?;
        }
        if self.if_missing {
            struct_ser.serialize_field("if_missing", &self.if_missing)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateVectorIndexRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "spec",
            "target_list_len",
            "targetListLen",
            "nprobe",
            "reassign_neighbours",
            "reassignNeighbours",
            "rebalance",
            "if_missing",
            "ifMissing",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Spec,
            TargetListLen,
            Nprobe,
            ReassignNeighbours,
            Rebalance,
            IfMissing,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "spec" => Ok(GeneratedField::Spec),
                            "targetListLen" | "target_list_len" => Ok(GeneratedField::TargetListLen),
                            "nprobe" => Ok(GeneratedField::Nprobe),
                            "reassignNeighbours" | "reassign_neighbours" => Ok(GeneratedField::ReassignNeighbours),
                            "rebalance" => Ok(GeneratedField::Rebalance),
                            "ifMissing" | "if_missing" => Ok(GeneratedField::IfMissing),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateVectorIndexRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.CreateVectorIndexRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CreateVectorIndexRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut spec__ = None;
                let mut target_list_len__ = None;
                let mut nprobe__ = None;
                let mut reassign_neighbours__ = None;
                let mut rebalance__ = None;
                let mut if_missing__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::TargetListLen => {
                            if target_list_len__.is_some() {
                                return Err(serde::de::Error::duplicate_field("targetListLen"));
                            }
                            target_list_len__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Nprobe => {
                            if nprobe__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nprobe"));
                            }
                            nprobe__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::ReassignNeighbours => {
                            if reassign_neighbours__.is_some() {
                                return Err(serde::de::Error::duplicate_field("reassignNeighbours"));
                            }
                            reassign_neighbours__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Rebalance => {
                            if rebalance__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rebalance"));
                            }
                            rebalance__ = Some(map_.next_value::<Rebalance>()? as i32);
                        }
                        GeneratedField::IfMissing => {
                            if if_missing__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ifMissing"));
                            }
                            if_missing__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CreateVectorIndexRequest {
                    name: name__.unwrap_or_default(),
                    spec: spec__,
                    target_list_len: target_list_len__,
                    nprobe: nprobe__,
                    reassign_neighbours: reassign_neighbours__,
                    rebalance: rebalance__.unwrap_or_default(),
                    if_missing: if_missing__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.CreateVectorIndexRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DeleteCell {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.family.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        if self.timestamp != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.DeleteCell", len)?;
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        if self.timestamp != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("timestamp", ToString::to_string(&self.timestamp).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DeleteCell {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "family",
            "qualifier",
            "timestamp",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Family,
            Qualifier,
            Timestamp,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "family" => Ok(GeneratedField::Family),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DeleteCell;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.DeleteCell")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DeleteCell, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut family__ = None;
                let mut qualifier__ = None;
                let mut timestamp__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(DeleteCell {
                    family: family__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                    timestamp: timestamp__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.DeleteCell", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DeleteColumn {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.family.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.DeleteColumn", len)?;
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DeleteColumn {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "family",
            "qualifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Family,
            Qualifier,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "family" => Ok(GeneratedField::Family),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DeleteColumn;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.DeleteColumn")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DeleteColumn, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut family__ = None;
                let mut qualifier__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(DeleteColumn {
                    family: family__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.DeleteColumn", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DeleteFamily {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.family.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.DeleteFamily", len)?;
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DeleteFamily {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "family",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Family,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "family" => Ok(GeneratedField::Family),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DeleteFamily;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.DeleteFamily")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DeleteFamily, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut family__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(DeleteFamily {
                    family: family__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.DeleteFamily", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DeleteRow {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("pigeonhole.v1.DeleteRow", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DeleteRow {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DeleteRow;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.DeleteRow")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DeleteRow, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(DeleteRow {
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.DeleteRow", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DescribeTableRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.DescribeTableRequest", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DescribeTableRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DescribeTableRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.DescribeTableRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DescribeTableRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(DescribeTableRequest {
                    name: name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.DescribeTableRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropTableRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.DropTableRequest", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropTableRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropTableRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.DropTableRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DropTableRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(DropTableRequest {
                    name: name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.DropTableRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Durability {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "DURABILITY_UNSPECIFIED",
            Self::None => "DURABILITY_NONE",
            Self::Buffered => "DURABILITY_BUFFERED",
            Self::GroupSync => "DURABILITY_GROUP_SYNC",
            Self::Sync => "DURABILITY_SYNC",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Durability {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "DURABILITY_UNSPECIFIED",
            "DURABILITY_NONE",
            "DURABILITY_BUFFERED",
            "DURABILITY_GROUP_SYNC",
            "DURABILITY_SYNC",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = Durability;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "DURABILITY_UNSPECIFIED" => Ok(Durability::Unspecified),
                    "DURABILITY_NONE" => Ok(Durability::None),
                    "DURABILITY_BUFFERED" => Ok(Durability::Buffered),
                    "DURABILITY_GROUP_SYNC" => Ok(Durability::GroupSync),
                    "DURABILITY_SYNC" => Ok(Durability::Sync),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Element {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "ELEMENT_UNSPECIFIED",
            Self::F32 => "ELEMENT_F32",
            Self::F16 => "ELEMENT_F16",
            Self::Bf16 => "ELEMENT_BF16",
            Self::I8 => "ELEMENT_I8",
            Self::Bit => "ELEMENT_BIT",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Element {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ELEMENT_UNSPECIFIED",
            "ELEMENT_F32",
            "ELEMENT_F16",
            "ELEMENT_BF16",
            "ELEMENT_I8",
            "ELEMENT_BIT",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = Element;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "ELEMENT_UNSPECIFIED" => Ok(Element::Unspecified),
                    "ELEMENT_F32" => Ok(Element::F32),
                    "ELEMENT_F16" => Ok(Element::F16),
                    "ELEMENT_BF16" => Ok(Element::Bf16),
                    "ELEMENT_I8" => Ok(Element::I8),
                    "ELEMENT_BIT" => Ok(Element::Bit),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Empty {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("pigeonhole.v1.Empty", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Empty {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Empty;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Empty")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Empty, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(Empty {
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Empty", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ErrorDetail {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.code.is_empty() {
            len += 1;
        }
        if self.code_num != 0 {
            len += 1;
        }
        if !self.message.is_empty() {
            len += 1;
        }
        if self.retry_safe {
            len += 1;
        }
        if self.retry_after_ms != 0 {
            len += 1;
        }
        if self.retry_from != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ErrorDetail", len)?;
        if !self.code.is_empty() {
            struct_ser.serialize_field("code", &self.code)?;
        }
        if self.code_num != 0 {
            struct_ser.serialize_field("code_num", &self.code_num)?;
        }
        if !self.message.is_empty() {
            struct_ser.serialize_field("message", &self.message)?;
        }
        if self.retry_safe {
            struct_ser.serialize_field("retry_safe", &self.retry_safe)?;
        }
        if self.retry_after_ms != 0 {
            struct_ser.serialize_field("retry_after_ms", &self.retry_after_ms)?;
        }
        if self.retry_from != 0 {
            let v = RetryFrom::try_from(self.retry_from)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.retry_from)))?;
            struct_ser.serialize_field("retry_from", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ErrorDetail {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "code",
            "code_num",
            "codeNum",
            "message",
            "retry_safe",
            "retrySafe",
            "retry_after_ms",
            "retryAfterMs",
            "retry_from",
            "retryFrom",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Code,
            CodeNum,
            Message,
            RetrySafe,
            RetryAfterMs,
            RetryFrom,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "code" => Ok(GeneratedField::Code),
                            "codeNum" | "code_num" => Ok(GeneratedField::CodeNum),
                            "message" => Ok(GeneratedField::Message),
                            "retrySafe" | "retry_safe" => Ok(GeneratedField::RetrySafe),
                            "retryAfterMs" | "retry_after_ms" => Ok(GeneratedField::RetryAfterMs),
                            "retryFrom" | "retry_from" => Ok(GeneratedField::RetryFrom),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ErrorDetail;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ErrorDetail")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ErrorDetail, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut code__ = None;
                let mut code_num__ = None;
                let mut message__ = None;
                let mut retry_safe__ = None;
                let mut retry_after_ms__ = None;
                let mut retry_from__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Code => {
                            if code__.is_some() {
                                return Err(serde::de::Error::duplicate_field("code"));
                            }
                            code__ = Some(map_.next_value()?);
                        }
                        GeneratedField::CodeNum => {
                            if code_num__.is_some() {
                                return Err(serde::de::Error::duplicate_field("codeNum"));
                            }
                            code_num__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Message => {
                            if message__.is_some() {
                                return Err(serde::de::Error::duplicate_field("message"));
                            }
                            message__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RetrySafe => {
                            if retry_safe__.is_some() {
                                return Err(serde::de::Error::duplicate_field("retrySafe"));
                            }
                            retry_safe__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RetryAfterMs => {
                            if retry_after_ms__.is_some() {
                                return Err(serde::de::Error::duplicate_field("retryAfterMs"));
                            }
                            retry_after_ms__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::RetryFrom => {
                            if retry_from__.is_some() {
                                return Err(serde::de::Error::duplicate_field("retryFrom"));
                            }
                            retry_from__ = Some(map_.next_value::<RetryFrom>()? as i32);
                        }
                    }
                }
                Ok(ErrorDetail {
                    code: code__.unwrap_or_default(),
                    code_num: code_num__.unwrap_or_default(),
                    message: message__.unwrap_or_default(),
                    retry_safe: retry_safe__.unwrap_or_default(),
                    retry_after_ms: retry_after_ms__.unwrap_or_default(),
                    retry_from: retry_from__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ErrorDetail", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FamilySpec {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.max_versions.is_some() {
            len += 1;
        }
        if self.ttl_micros.is_some() {
            len += 1;
        }
        if self.bloom_bits.is_some() {
            len += 1;
        }
        if self.blob_threshold.is_some() {
            len += 1;
        }
        if self.compression.is_some() {
            len += 1;
        }
        if self.block_size.is_some() {
            len += 1;
        }
        if self.cache_priority != 0 {
            len += 1;
        }
        if self.compaction != 0 {
            len += 1;
        }
        if self.kind.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.FamilySpec", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.max_versions.as_ref() {
            struct_ser.serialize_field("max_versions", v)?;
        }
        if let Some(v) = self.ttl_micros.as_ref() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("ttl_micros", ToString::to_string(&v).as_str())?;
        }
        if let Some(v) = self.bloom_bits.as_ref() {
            struct_ser.serialize_field("bloom_bits", v)?;
        }
        if let Some(v) = self.blob_threshold.as_ref() {
            struct_ser.serialize_field("blob_threshold", v)?;
        }
        if let Some(v) = self.compression.as_ref() {
            struct_ser.serialize_field("compression", v)?;
        }
        if let Some(v) = self.block_size.as_ref() {
            struct_ser.serialize_field("block_size", v)?;
        }
        if self.cache_priority != 0 {
            let v = Priority::try_from(self.cache_priority)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.cache_priority)))?;
            struct_ser.serialize_field("cache_priority", &v)?;
        }
        if self.compaction != 0 {
            let v = CompactionStyle::try_from(self.compaction)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.compaction)))?;
            struct_ser.serialize_field("compaction", &v)?;
        }
        if let Some(v) = self.kind.as_ref() {
            match v {
                family_spec::Kind::Aggregate(v) => {
                    struct_ser.serialize_field("aggregate", v)?;
                }
                family_spec::Kind::Vector(v) => {
                    struct_ser.serialize_field("vector", v)?;
                }
                family_spec::Kind::MergeOperator(v) => {
                    struct_ser.serialize_field("merge_operator", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FamilySpec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "max_versions",
            "maxVersions",
            "ttl_micros",
            "ttlMicros",
            "bloom_bits",
            "bloomBits",
            "blob_threshold",
            "blobThreshold",
            "compression",
            "block_size",
            "blockSize",
            "cache_priority",
            "cachePriority",
            "compaction",
            "aggregate",
            "vector",
            "merge_operator",
            "mergeOperator",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            MaxVersions,
            TtlMicros,
            BloomBits,
            BlobThreshold,
            Compression,
            BlockSize,
            CachePriority,
            Compaction,
            Aggregate,
            Vector,
            MergeOperator,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "maxVersions" | "max_versions" => Ok(GeneratedField::MaxVersions),
                            "ttlMicros" | "ttl_micros" => Ok(GeneratedField::TtlMicros),
                            "bloomBits" | "bloom_bits" => Ok(GeneratedField::BloomBits),
                            "blobThreshold" | "blob_threshold" => Ok(GeneratedField::BlobThreshold),
                            "compression" => Ok(GeneratedField::Compression),
                            "blockSize" | "block_size" => Ok(GeneratedField::BlockSize),
                            "cachePriority" | "cache_priority" => Ok(GeneratedField::CachePriority),
                            "compaction" => Ok(GeneratedField::Compaction),
                            "aggregate" => Ok(GeneratedField::Aggregate),
                            "vector" => Ok(GeneratedField::Vector),
                            "mergeOperator" | "merge_operator" => Ok(GeneratedField::MergeOperator),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FamilySpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.FamilySpec")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FamilySpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut max_versions__ = None;
                let mut ttl_micros__ = None;
                let mut bloom_bits__ = None;
                let mut blob_threshold__ = None;
                let mut compression__ = None;
                let mut block_size__ = None;
                let mut cache_priority__ = None;
                let mut compaction__ = None;
                let mut kind__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::MaxVersions => {
                            if max_versions__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxVersions"));
                            }
                            max_versions__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::TtlMicros => {
                            if ttl_micros__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ttlMicros"));
                            }
                            ttl_micros__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::BloomBits => {
                            if bloom_bits__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomBits"));
                            }
                            bloom_bits__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::BlobThreshold => {
                            if blob_threshold__.is_some() {
                                return Err(serde::de::Error::duplicate_field("blobThreshold"));
                            }
                            blob_threshold__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Compression => {
                            if compression__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compression"));
                            }
                            compression__ = map_.next_value()?;
                        }
                        GeneratedField::BlockSize => {
                            if block_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("blockSize"));
                            }
                            block_size__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::CachePriority => {
                            if cache_priority__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cachePriority"));
                            }
                            cache_priority__ = Some(map_.next_value::<Priority>()? as i32);
                        }
                        GeneratedField::Compaction => {
                            if compaction__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compaction"));
                            }
                            compaction__ = Some(map_.next_value::<CompactionStyle>()? as i32);
                        }
                        GeneratedField::Aggregate => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregate"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<_>>()?.map(family_spec::Kind::Aggregate)
;
                        }
                        GeneratedField::Vector => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("vector"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<_>>()?.map(family_spec::Kind::Vector)
;
                        }
                        GeneratedField::MergeOperator => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("mergeOperator"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<_>>()?.map(family_spec::Kind::MergeOperator);
                        }
                    }
                }
                Ok(FamilySpec {
                    name: name__.unwrap_or_default(),
                    max_versions: max_versions__,
                    ttl_micros: ttl_micros__,
                    bloom_bits: bloom_bits__,
                    blob_threshold: blob_threshold__,
                    compression: compression__,
                    block_size: block_size__,
                    cache_priority: cache_priority__.unwrap_or_default(),
                    compaction: compaction__.unwrap_or_default(),
                    kind: kind__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.FamilySpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FeedCommit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.seqno != 0 {
            len += 1;
        }
        if self.timestamp != 0 {
            len += 1;
        }
        if !self.changes.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.FeedCommit", len)?;
        if self.seqno != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("seqno", ToString::to_string(&self.seqno).as_str())?;
        }
        if self.timestamp != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("timestamp", ToString::to_string(&self.timestamp).as_str())?;
        }
        if !self.changes.is_empty() {
            struct_ser.serialize_field("changes", &self.changes)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FeedCommit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "seqno",
            "timestamp",
            "changes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Seqno,
            Timestamp,
            Changes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "seqno" => Ok(GeneratedField::Seqno),
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            "changes" => Ok(GeneratedField::Changes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FeedCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.FeedCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FeedCommit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut seqno__ = None;
                let mut timestamp__ = None;
                let mut changes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Seqno => {
                            if seqno__.is_some() {
                                return Err(serde::de::Error::duplicate_field("seqno"));
                            }
                            seqno__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Changes => {
                            if changes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("changes"));
                            }
                            changes__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FeedCommit {
                    seqno: seqno__.unwrap_or_default(),
                    timestamp: timestamp__.unwrap_or_default(),
                    changes: changes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.FeedCommit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FeedEvent {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.token.is_empty() {
            len += 1;
        }
        if self.event.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.FeedEvent", len)?;
        if !self.token.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("token", pbjson::private::base64::encode(&self.token).as_str())?;
        }
        if let Some(v) = self.event.as_ref() {
            match v {
                feed_event::Event::Commit(v) => {
                    struct_ser.serialize_field("commit", v)?;
                }
                feed_event::Event::Progress(v) => {
                    struct_ser.serialize_field("progress", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FeedEvent {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "token",
            "commit",
            "progress",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Token,
            Commit,
            Progress,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "token" => Ok(GeneratedField::Token),
                            "commit" => Ok(GeneratedField::Commit),
                            "progress" => Ok(GeneratedField::Progress),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FeedEvent;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.FeedEvent")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FeedEvent, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut token__ = None;
                let mut event__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Token => {
                            if token__.is_some() {
                                return Err(serde::de::Error::duplicate_field("token"));
                            }
                            token__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Commit => {
                            if event__.is_some() {
                                return Err(serde::de::Error::duplicate_field("commit"));
                            }
                            event__ = map_.next_value::<::std::option::Option<_>>()?.map(feed_event::Event::Commit)
;
                        }
                        GeneratedField::Progress => {
                            if event__.is_some() {
                                return Err(serde::de::Error::duplicate_field("progress"));
                            }
                            event__ = map_.next_value::<::std::option::Option<_>>()?.map(feed_event::Event::Progress)
;
                        }
                    }
                }
                Ok(FeedEvent {
                    token: token__.unwrap_or_default(),
                    event: event__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.FeedEvent", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ForgetRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.ids.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ForgetRequest", len)?;
        if !self.ids.is_empty() {
            struct_ser.serialize_field("ids", &self.ids.iter().map(pbjson::private::base64::encode).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ForgetRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ids",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Ids,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "ids" => Ok(GeneratedField::Ids),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ForgetRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ForgetRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ForgetRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut ids__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Ids => {
                            if ids__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ids"));
                            }
                            ids__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(ForgetRequest {
                    ids: ids__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ForgetRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ForgetResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.forgotten != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ForgetResponse", len)?;
        if self.forgotten != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("forgotten", ToString::to_string(&self.forgotten).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ForgetResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "forgotten",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Forgotten,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "forgotten" => Ok(GeneratedField::Forgotten),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ForgetResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ForgetResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ForgetResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut forgotten__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Forgotten => {
                            if forgotten__.is_some() {
                                return Err(serde::de::Error::duplicate_field("forgotten"));
                            }
                            forgotten__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ForgetResponse {
                    forgotten: forgotten__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ForgetResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Fuzziness {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "FUZZINESS_UNSPECIFIED",
            Self::Auto => "FUZZINESS_AUTO",
            Self::One => "FUZZINESS_ONE",
            Self::Two => "FUZZINESS_TWO",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Fuzziness {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "FUZZINESS_UNSPECIFIED",
            "FUZZINESS_AUTO",
            "FUZZINESS_ONE",
            "FUZZINESS_TWO",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = Fuzziness;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "FUZZINESS_UNSPECIFIED" => Ok(Fuzziness::Unspecified),
                    "FUZZINESS_AUTO" => Ok(Fuzziness::Auto),
                    "FUZZINESS_ONE" => Ok(Fuzziness::One),
                    "FUZZINESS_TWO" => Ok(Fuzziness::Two),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Fuzzy {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.term.is_empty() {
            len += 1;
        }
        if self.fuzziness != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Fuzzy", len)?;
        if !self.term.is_empty() {
            struct_ser.serialize_field("term", &self.term)?;
        }
        if self.fuzziness != 0 {
            let v = Fuzziness::try_from(self.fuzziness)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.fuzziness)))?;
            struct_ser.serialize_field("fuzziness", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Fuzzy {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "term",
            "fuzziness",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Term,
            Fuzziness,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "term" => Ok(GeneratedField::Term),
                            "fuzziness" => Ok(GeneratedField::Fuzziness),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Fuzzy;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Fuzzy")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Fuzzy, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut term__ = None;
                let mut fuzziness__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Term => {
                            if term__.is_some() {
                                return Err(serde::de::Error::duplicate_field("term"));
                            }
                            term__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Fuzziness => {
                            if fuzziness__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fuzziness"));
                            }
                            fuzziness__ = Some(map_.next_value::<Fuzziness>()? as i32);
                        }
                    }
                }
                Ok(Fuzzy {
                    term: term__.unwrap_or_default(),
                    fuzziness: fuzziness__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Fuzzy", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table.is_empty() {
            len += 1;
        }
        if !self.row.is_empty() {
            len += 1;
        }
        if !self.family.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        if !self.snapshot_id.is_empty() {
            len += 1;
        }
        if self.aggregate.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.GetRequest", len)?;
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        if !self.row.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("row", pbjson::private::base64::encode(&self.row).as_str())?;
        }
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        if !self.snapshot_id.is_empty() {
            struct_ser.serialize_field("snapshot_id", &self.snapshot_id)?;
        }
        if let Some(v) = self.aggregate.as_ref() {
            struct_ser.serialize_field("aggregate", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table",
            "row",
            "family",
            "qualifier",
            "snapshot_id",
            "snapshotId",
            "aggregate",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Table,
            Row,
            Family,
            Qualifier,
            SnapshotId,
            Aggregate,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "table" => Ok(GeneratedField::Table),
                            "row" => Ok(GeneratedField::Row),
                            "family" => Ok(GeneratedField::Family),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            "snapshotId" | "snapshot_id" => Ok(GeneratedField::SnapshotId),
                            "aggregate" => Ok(GeneratedField::Aggregate),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.GetRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<GetRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table__ = None;
                let mut row__ = None;
                let mut family__ = None;
                let mut qualifier__ = None;
                let mut snapshot_id__ = None;
                let mut aggregate__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Row => {
                            if row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("row"));
                            }
                            row__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SnapshotId => {
                            if snapshot_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotId"));
                            }
                            snapshot_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Aggregate => {
                            if aggregate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregate"));
                            }
                            aggregate__ = map_.next_value()?;
                        }
                    }
                }
                Ok(GetRequest {
                    table: table__.unwrap_or_default(),
                    row: row__.unwrap_or_default(),
                    family: family__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                    snapshot_id: snapshot_id__.unwrap_or_default(),
                    aggregate: aggregate__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.GetRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.cell.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.GetResponse", len)?;
        if let Some(v) = self.cell.as_ref() {
            struct_ser.serialize_field("cell", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "cell",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Cell,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "cell" => Ok(GeneratedField::Cell),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.GetResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<GetResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut cell__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Cell => {
                            if cell__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cell"));
                            }
                            cell__ = map_.next_value()?;
                        }
                    }
                }
                Ok(GetResponse {
                    cell: cell__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.GetResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HealthResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.status.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.HealthResponse", len)?;
        if !self.status.is_empty() {
            struct_ser.serialize_field("status", &self.status)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HealthResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "status" => Ok(GeneratedField::Status),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HealthResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.HealthResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<HealthResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(HealthResponse {
                    status: status__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.HealthResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Hop {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.nodes.is_empty() {
            len += 1;
        }
        if !self.parents.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Hop", len)?;
        if !self.nodes.is_empty() {
            struct_ser.serialize_field("nodes", &self.nodes.iter().map(pbjson::private::base64::encode).collect::<Vec<_>>())?;
        }
        if !self.parents.is_empty() {
            struct_ser.serialize_field("parents", &self.parents)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Hop {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "nodes",
            "parents",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Nodes,
            Parents,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "nodes" => Ok(GeneratedField::Nodes),
                            "parents" => Ok(GeneratedField::Parents),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Hop;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Hop")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Hop, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut nodes__ = None;
                let mut parents__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Nodes => {
                            if nodes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodes"));
                            }
                            nodes__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::Parents => {
                            if parents__.is_some() {
                                return Err(serde::de::Error::duplicate_field("parents"));
                            }
                            parents__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(Hop {
                    nodes: nodes__.unwrap_or_default(),
                    parents: parents__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Hop", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for I64Comparison {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.ordering != 0 {
            len += 1;
        }
        if self.operand != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.I64Comparison", len)?;
        if self.ordering != 0 {
            let v = Ordering::try_from(self.ordering)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.ordering)))?;
            struct_ser.serialize_field("ordering", &v)?;
        }
        if self.operand != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("operand", ToString::to_string(&self.operand).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for I64Comparison {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ordering",
            "operand",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Ordering,
            Operand,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "ordering" => Ok(GeneratedField::Ordering),
                            "operand" => Ok(GeneratedField::Operand),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = I64Comparison;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.I64Comparison")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<I64Comparison, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut ordering__ = None;
                let mut operand__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Ordering => {
                            if ordering__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ordering"));
                            }
                            ordering__ = Some(map_.next_value::<Ordering>()? as i32);
                        }
                        GeneratedField::Operand => {
                            if operand__.is_some() {
                                return Err(serde::de::Error::duplicate_field("operand"));
                            }
                            operand__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(I64Comparison {
                    ordering: ordering__.unwrap_or_default(),
                    operand: operand__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.I64Comparison", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Incr {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.family.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        if self.timestamp.is_some() {
            len += 1;
        }
        if self.delta != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Incr", len)?;
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        if let Some(v) = self.timestamp.as_ref() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("timestamp", ToString::to_string(&v).as_str())?;
        }
        if self.delta != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("delta", ToString::to_string(&self.delta).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Incr {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "family",
            "qualifier",
            "timestamp",
            "delta",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Family,
            Qualifier,
            Timestamp,
            Delta,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "family" => Ok(GeneratedField::Family),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            "delta" => Ok(GeneratedField::Delta),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Incr;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Incr")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Incr, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut family__ = None;
                let mut qualifier__ = None;
                let mut timestamp__ = None;
                let mut delta__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Delta => {
                            if delta__.is_some() {
                                return Err(serde::de::Error::duplicate_field("delta"));
                            }
                            delta__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Incr {
                    family: family__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                    timestamp: timestamp__,
                    delta: delta__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Incr", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for InfoResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.path.is_empty() {
            len += 1;
        }
        if self.file_bytes != 0 {
            len += 1;
        }
        if self.free_bytes != 0 {
            len += 1;
        }
        if !self.tables.is_empty() {
            len += 1;
        }
        if !self.version.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.InfoResponse", len)?;
        if !self.path.is_empty() {
            struct_ser.serialize_field("path", &self.path)?;
        }
        if self.file_bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("file_bytes", ToString::to_string(&self.file_bytes).as_str())?;
        }
        if self.free_bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("free_bytes", ToString::to_string(&self.free_bytes).as_str())?;
        }
        if !self.tables.is_empty() {
            struct_ser.serialize_field("tables", &self.tables)?;
        }
        if !self.version.is_empty() {
            struct_ser.serialize_field("version", &self.version)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for InfoResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "path",
            "file_bytes",
            "fileBytes",
            "free_bytes",
            "freeBytes",
            "tables",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Path,
            FileBytes,
            FreeBytes,
            Tables,
            Version,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "path" => Ok(GeneratedField::Path),
                            "fileBytes" | "file_bytes" => Ok(GeneratedField::FileBytes),
                            "freeBytes" | "free_bytes" => Ok(GeneratedField::FreeBytes),
                            "tables" => Ok(GeneratedField::Tables),
                            "version" => Ok(GeneratedField::Version),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = InfoResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.InfoResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<InfoResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut path__ = None;
                let mut file_bytes__ = None;
                let mut free_bytes__ = None;
                let mut tables__ = None;
                let mut version__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Path => {
                            if path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("path"));
                            }
                            path__ = Some(map_.next_value()?);
                        }
                        GeneratedField::FileBytes => {
                            if file_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fileBytes"));
                            }
                            file_bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::FreeBytes => {
                            if free_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("freeBytes"));
                            }
                            free_bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Tables => {
                            if tables__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tables"));
                            }
                            tables__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Version => {
                            if version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(InfoResponse {
                    path: path__.unwrap_or_default(),
                    file_bytes: file_bytes__.unwrap_or_default(),
                    free_bytes: free_bytes__.unwrap_or_default(),
                    tables: tables__.unwrap_or_default(),
                    version: version__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.InfoResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for KeyList {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.keys.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.KeyList", len)?;
        if !self.keys.is_empty() {
            struct_ser.serialize_field("keys", &self.keys.iter().map(pbjson::private::base64::encode).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for KeyList {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "keys",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Keys,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "keys" => Ok(GeneratedField::Keys),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = KeyList;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.KeyList")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<KeyList, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut keys__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Keys => {
                            if keys__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keys"));
                            }
                            keys__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(KeyList {
                    keys: keys__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.KeyList", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for KeyRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.range.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.KeyRange", len)?;
        if let Some(v) = self.range.as_ref() {
            match v {
                key_range::Range::Prefix(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("prefix", pbjson::private::base64::encode(&v).as_str())?;
                }
                key_range::Range::Bounds(v) => {
                    struct_ser.serialize_field("bounds", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for KeyRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "prefix",
            "bounds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Prefix,
            Bounds,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "prefix" => Ok(GeneratedField::Prefix),
                            "bounds" => Ok(GeneratedField::Bounds),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = KeyRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.KeyRange")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<KeyRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut range__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Prefix => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("prefix"));
                            }
                            range__ = map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| key_range::Range::Prefix(x.0));
                        }
                        GeneratedField::Bounds => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bounds"));
                            }
                            range__ = map_.next_value::<::std::option::Option<_>>()?.map(key_range::Range::Bounds)
;
                        }
                    }
                }
                Ok(KeyRange {
                    range: range__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.KeyRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LinkRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.from.is_empty() {
            len += 1;
        }
        if !self.to.is_empty() {
            len += 1;
        }
        if self.link_type.is_some() {
            len += 1;
        }
        if self.weight != 0. {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.LinkRequest", len)?;
        if !self.from.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("from", pbjson::private::base64::encode(&self.from).as_str())?;
        }
        if !self.to.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("to", pbjson::private::base64::encode(&self.to).as_str())?;
        }
        if let Some(v) = self.link_type.as_ref() {
            struct_ser.serialize_field("link_type", v)?;
        }
        if self.weight != 0. {
            struct_ser.serialize_field("weight", &self.weight)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LinkRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "from",
            "to",
            "link_type",
            "linkType",
            "weight",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            From,
            To,
            LinkType,
            Weight,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "from" => Ok(GeneratedField::From),
                            "to" => Ok(GeneratedField::To),
                            "linkType" | "link_type" => Ok(GeneratedField::LinkType),
                            "weight" => Ok(GeneratedField::Weight),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LinkRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.LinkRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LinkRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut from__ = None;
                let mut to__ = None;
                let mut link_type__ = None;
                let mut weight__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::From => {
                            if from__.is_some() {
                                return Err(serde::de::Error::duplicate_field("from"));
                            }
                            from__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::To => {
                            if to__.is_some() {
                                return Err(serde::de::Error::duplicate_field("to"));
                            }
                            to__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LinkType => {
                            if link_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("linkType"));
                            }
                            link_type__ = map_.next_value()?;
                        }
                        GeneratedField::Weight => {
                            if weight__.is_some() {
                                return Err(serde::de::Error::duplicate_field("weight"));
                            }
                            weight__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(LinkRequest {
                    from: from__.unwrap_or_default(),
                    to: to__.unwrap_or_default(),
                    link_type: link_type__,
                    weight: weight__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.LinkRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LinkType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.kind.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.LinkType", len)?;
        if let Some(v) = self.kind.as_ref() {
            match v {
                link_type::Kind::Builtin(v) => {
                    let v = BuiltinLink::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("builtin", &v)?;
                }
                link_type::Kind::Custom(v) => {
                    struct_ser.serialize_field("custom", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LinkType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "builtin",
            "custom",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Builtin,
            Custom,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "builtin" => Ok(GeneratedField::Builtin),
                            "custom" => Ok(GeneratedField::Custom),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LinkType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.LinkType")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LinkType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut kind__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Builtin => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("builtin"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<BuiltinLink>>()?.map(|x| link_type::Kind::Builtin(x as i32));
                        }
                        GeneratedField::Custom => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("custom"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| link_type::Kind::Custom(x.0));
                        }
                    }
                }
                Ok(LinkType {
                    kind: kind__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.LinkType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListTablesRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("pigeonhole.v1.ListTablesRequest", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListTablesRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListTablesRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ListTablesRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListTablesRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(ListTablesRequest {
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ListTablesRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListTablesResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.tables.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ListTablesResponse", len)?;
        if !self.tables.is_empty() {
            struct_ser.serialize_field("tables", &self.tables)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListTablesResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tables",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Tables,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tables" => Ok(GeneratedField::Tables),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListTablesResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ListTablesResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListTablesResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut tables__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Tables => {
                            if tables__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tables"));
                            }
                            tables__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ListTablesResponse {
                    tables: tables__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ListTablesResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MarkUsedRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.ids.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MarkUsedRequest", len)?;
        if !self.ids.is_empty() {
            struct_ser.serialize_field("ids", &self.ids.iter().map(pbjson::private::base64::encode).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MarkUsedRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ids",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Ids,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "ids" => Ok(GeneratedField::Ids),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MarkUsedRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MarkUsedRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MarkUsedRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut ids__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Ids => {
                            if ids__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ids"));
                            }
                            ids__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(MarkUsedRequest {
                    ids: ids__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MarkUsedRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MarkUsedResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.marked != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MarkUsedResponse", len)?;
        if self.marked != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("marked", ToString::to_string(&self.marked).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MarkUsedResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "marked",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Marked,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "marked" => Ok(GeneratedField::Marked),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MarkUsedResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MarkUsedResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MarkUsedResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut marked__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Marked => {
                            if marked__.is_some() {
                                return Err(serde::de::Error::duplicate_field("marked"));
                            }
                            marked__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(MarkUsedResponse {
                    marked: marked__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MarkUsedResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MeanVar {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.count != 0 {
            len += 1;
        }
        if self.mean != 0. {
            len += 1;
        }
        if self.variance != 0. {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MeanVar", len)?;
        if self.count != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("count", ToString::to_string(&self.count).as_str())?;
        }
        if self.mean != 0. {
            struct_ser.serialize_field("mean", &self.mean)?;
        }
        if self.variance != 0. {
            struct_ser.serialize_field("variance", &self.variance)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MeanVar {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "count",
            "mean",
            "variance",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Count,
            Mean,
            Variance,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "count" => Ok(GeneratedField::Count),
                            "mean" => Ok(GeneratedField::Mean),
                            "variance" => Ok(GeneratedField::Variance),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MeanVar;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MeanVar")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MeanVar, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut count__ = None;
                let mut mean__ = None;
                let mut variance__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Count => {
                            if count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("count"));
                            }
                            count__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Mean => {
                            if mean__.is_some() {
                                return Err(serde::de::Error::duplicate_field("mean"));
                            }
                            mean__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Variance => {
                            if variance__.is_some() {
                                return Err(serde::de::Error::duplicate_field("variance"));
                            }
                            variance__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(MeanVar {
                    count: count__.unwrap_or_default(),
                    mean: mean__.unwrap_or_default(),
                    variance: variance__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MeanVar", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MemoryEvent {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.token.is_empty() {
            len += 1;
        }
        if self.event.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MemoryEvent", len)?;
        if !self.token.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("token", pbjson::private::base64::encode(&self.token).as_str())?;
        }
        if let Some(v) = self.event.as_ref() {
            match v {
                memory_event::Event::Memory(v) => {
                    struct_ser.serialize_field("memory", v)?;
                }
                memory_event::Event::Progress(v) => {
                    struct_ser.serialize_field("progress", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MemoryEvent {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "token",
            "memory",
            "progress",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Token,
            Memory,
            Progress,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "token" => Ok(GeneratedField::Token),
                            "memory" => Ok(GeneratedField::Memory),
                            "progress" => Ok(GeneratedField::Progress),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MemoryEvent;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MemoryEvent")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MemoryEvent, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut token__ = None;
                let mut event__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Token => {
                            if token__.is_some() {
                                return Err(serde::de::Error::duplicate_field("token"));
                            }
                            token__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Memory => {
                            if event__.is_some() {
                                return Err(serde::de::Error::duplicate_field("memory"));
                            }
                            event__ = map_.next_value::<::std::option::Option<_>>()?.map(memory_event::Event::Memory)
;
                        }
                        GeneratedField::Progress => {
                            if event__.is_some() {
                                return Err(serde::de::Error::duplicate_field("progress"));
                            }
                            event__ = map_.next_value::<::std::option::Option<_>>()?.map(memory_event::Event::Progress)
;
                        }
                    }
                }
                Ok(MemoryEvent {
                    token: token__.unwrap_or_default(),
                    event: event__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MemoryEvent", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MemoryHit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if !self.text.is_empty() {
            len += 1;
        }
        if self.kind != 0 {
            len += 1;
        }
        if self.importance != 0. {
            len += 1;
        }
        if self.created != 0 {
            len += 1;
        }
        if self.superseded {
            len += 1;
        }
        if self.score != 0. {
            len += 1;
        }
        if self.embedding.is_some() {
            len += 1;
        }
        if !self.signals.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MemoryHit", len)?;
        if !self.id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id", pbjson::private::base64::encode(&self.id).as_str())?;
        }
        if !self.text.is_empty() {
            struct_ser.serialize_field("text", &self.text)?;
        }
        if self.kind != 0 {
            let v = MemoryKind::try_from(self.kind)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.kind)))?;
            struct_ser.serialize_field("kind", &v)?;
        }
        if self.importance != 0. {
            struct_ser.serialize_field("importance", &self.importance)?;
        }
        if self.created != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("created", ToString::to_string(&self.created).as_str())?;
        }
        if self.superseded {
            struct_ser.serialize_field("superseded", &self.superseded)?;
        }
        if self.score != 0. {
            struct_ser.serialize_field("score", &self.score)?;
        }
        if let Some(v) = self.embedding.as_ref() {
            struct_ser.serialize_field("embedding", v)?;
        }
        if !self.signals.is_empty() {
            struct_ser.serialize_field("signals", &self.signals)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MemoryHit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "text",
            "kind",
            "importance",
            "created",
            "superseded",
            "score",
            "embedding",
            "signals",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Text,
            Kind,
            Importance,
            Created,
            Superseded,
            Score,
            Embedding,
            Signals,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "text" => Ok(GeneratedField::Text),
                            "kind" => Ok(GeneratedField::Kind),
                            "importance" => Ok(GeneratedField::Importance),
                            "created" => Ok(GeneratedField::Created),
                            "superseded" => Ok(GeneratedField::Superseded),
                            "score" => Ok(GeneratedField::Score),
                            "embedding" => Ok(GeneratedField::Embedding),
                            "signals" => Ok(GeneratedField::Signals),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MemoryHit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MemoryHit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MemoryHit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut text__ = None;
                let mut kind__ = None;
                let mut importance__ = None;
                let mut created__ = None;
                let mut superseded__ = None;
                let mut score__ = None;
                let mut embedding__ = None;
                let mut signals__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Text => {
                            if text__.is_some() {
                                return Err(serde::de::Error::duplicate_field("text"));
                            }
                            text__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Kind => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("kind"));
                            }
                            kind__ = Some(map_.next_value::<MemoryKind>()? as i32);
                        }
                        GeneratedField::Importance => {
                            if importance__.is_some() {
                                return Err(serde::de::Error::duplicate_field("importance"));
                            }
                            importance__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Created => {
                            if created__.is_some() {
                                return Err(serde::de::Error::duplicate_field("created"));
                            }
                            created__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Superseded => {
                            if superseded__.is_some() {
                                return Err(serde::de::Error::duplicate_field("superseded"));
                            }
                            superseded__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Score => {
                            if score__.is_some() {
                                return Err(serde::de::Error::duplicate_field("score"));
                            }
                            score__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Embedding => {
                            if embedding__.is_some() {
                                return Err(serde::de::Error::duplicate_field("embedding"));
                            }
                            embedding__ = map_.next_value()?;
                        }
                        GeneratedField::Signals => {
                            if signals__.is_some() {
                                return Err(serde::de::Error::duplicate_field("signals"));
                            }
                            signals__ = Some(
                                map_.next_value::<std::collections::HashMap<_, ::pbjson::private::NumberDeserialize<f64>>>()?
                                    .into_iter().map(|(k,v)| (k, v.0)).collect()
                            );
                        }
                    }
                }
                Ok(MemoryHit {
                    id: id__.unwrap_or_default(),
                    text: text__.unwrap_or_default(),
                    kind: kind__.unwrap_or_default(),
                    importance: importance__.unwrap_or_default(),
                    created: created__.unwrap_or_default(),
                    superseded: superseded__.unwrap_or_default(),
                    score: score__.unwrap_or_default(),
                    embedding: embedding__,
                    signals: signals__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MemoryHit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MemoryKind {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "MEMORY_KIND_UNSPECIFIED",
            Self::Episodic => "MEMORY_KIND_EPISODIC",
            Self::Semantic => "MEMORY_KIND_SEMANTIC",
            Self::Procedural => "MEMORY_KIND_PROCEDURAL",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for MemoryKind {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "MEMORY_KIND_UNSPECIFIED",
            "MEMORY_KIND_EPISODIC",
            "MEMORY_KIND_SEMANTIC",
            "MEMORY_KIND_PROCEDURAL",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = MemoryKind;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "MEMORY_KIND_UNSPECIFIED" => Ok(MemoryKind::Unspecified),
                    "MEMORY_KIND_EPISODIC" => Ok(MemoryKind::Episodic),
                    "MEMORY_KIND_SEMANTIC" => Ok(MemoryKind::Semantic),
                    "MEMORY_KIND_PROCEDURAL" => Ok(MemoryKind::Procedural),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for MemoryLink {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.to.is_empty() {
            len += 1;
        }
        if self.link_type.is_some() {
            len += 1;
        }
        if self.weight != 0. {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MemoryLink", len)?;
        if !self.to.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("to", pbjson::private::base64::encode(&self.to).as_str())?;
        }
        if let Some(v) = self.link_type.as_ref() {
            struct_ser.serialize_field("link_type", v)?;
        }
        if self.weight != 0. {
            struct_ser.serialize_field("weight", &self.weight)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MemoryLink {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "to",
            "link_type",
            "linkType",
            "weight",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            To,
            LinkType,
            Weight,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "to" => Ok(GeneratedField::To),
                            "linkType" | "link_type" => Ok(GeneratedField::LinkType),
                            "weight" => Ok(GeneratedField::Weight),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MemoryLink;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MemoryLink")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MemoryLink, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut to__ = None;
                let mut link_type__ = None;
                let mut weight__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::To => {
                            if to__.is_some() {
                                return Err(serde::de::Error::duplicate_field("to"));
                            }
                            to__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LinkType => {
                            if link_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("linkType"));
                            }
                            link_type__ = map_.next_value()?;
                        }
                        GeneratedField::Weight => {
                            if weight__.is_some() {
                                return Err(serde::de::Error::duplicate_field("weight"));
                            }
                            weight__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(MemoryLink {
                    to: to__.unwrap_or_default(),
                    link_type: link_type__,
                    weight: weight__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MemoryLink", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MemoryWatchRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.namespaces.is_empty() {
            len += 1;
        }
        if !self.resume.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MemoryWatchRequest", len)?;
        if !self.namespaces.is_empty() {
            struct_ser.serialize_field("namespaces", &self.namespaces)?;
        }
        if !self.resume.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("resume", pbjson::private::base64::encode(&self.resume).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MemoryWatchRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "namespaces",
            "resume",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Namespaces,
            Resume,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "namespaces" => Ok(GeneratedField::Namespaces),
                            "resume" => Ok(GeneratedField::Resume),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MemoryWatchRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MemoryWatchRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MemoryWatchRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut namespaces__ = None;
                let mut resume__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Namespaces => {
                            if namespaces__.is_some() {
                                return Err(serde::de::Error::duplicate_field("namespaces"));
                            }
                            namespaces__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Resume => {
                            if resume__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resume"));
                            }
                            resume__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(MemoryWatchRequest {
                    namespaces: namespaces__.unwrap_or_default(),
                    resume: resume__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MemoryWatchRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Merge {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.family.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        if !self.operand.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Merge", len)?;
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        if !self.operand.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("operand", pbjson::private::base64::encode(&self.operand).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Merge {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "family",
            "qualifier",
            "operand",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Family,
            Qualifier,
            Operand,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "family" => Ok(GeneratedField::Family),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            "operand" => Ok(GeneratedField::Operand),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Merge;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Merge")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Merge, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut family__ = None;
                let mut qualifier__ = None;
                let mut operand__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Operand => {
                            if operand__.is_some() {
                                return Err(serde::de::Error::duplicate_field("operand"));
                            }
                            operand__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Merge {
                    family: family__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                    operand: operand__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Merge", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Metric {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "METRIC_UNSPECIFIED",
            Self::L2 => "METRIC_L2",
            Self::Cosine => "METRIC_COSINE",
            Self::InnerProduct => "METRIC_INNER_PRODUCT",
            Self::Hamming => "METRIC_HAMMING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Metric {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "METRIC_UNSPECIFIED",
            "METRIC_L2",
            "METRIC_COSINE",
            "METRIC_INNER_PRODUCT",
            "METRIC_HAMMING",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = Metric;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "METRIC_UNSPECIFIED" => Ok(Metric::Unspecified),
                    "METRIC_L2" => Ok(Metric::L2),
                    "METRIC_COSINE" => Ok(Metric::Cosine),
                    "METRIC_INNER_PRODUCT" => Ok(Metric::InnerProduct),
                    "METRIC_HAMMING" => Ok(Metric::Hamming),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for MetricsResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.text.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MetricsResponse", len)?;
        if !self.text.is_empty() {
            struct_ser.serialize_field("text", &self.text)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MetricsResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "text",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Text,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "text" => Ok(GeneratedField::Text),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MetricsResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MetricsResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MetricsResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut text__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Text => {
                            if text__.is_some() {
                                return Err(serde::de::Error::duplicate_field("text"));
                            }
                            text__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(MetricsResponse {
                    text: text__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MetricsResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MultiGetRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table.is_empty() {
            len += 1;
        }
        if !self.keys.is_empty() {
            len += 1;
        }
        if self.selection.is_some() {
            len += 1;
        }
        if self.page_bytes != 0 {
            len += 1;
        }
        if !self.after.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MultiGetRequest", len)?;
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        if !self.keys.is_empty() {
            struct_ser.serialize_field("keys", &self.keys.iter().map(pbjson::private::base64::encode).collect::<Vec<_>>())?;
        }
        if let Some(v) = self.selection.as_ref() {
            struct_ser.serialize_field("selection", v)?;
        }
        if self.page_bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("page_bytes", ToString::to_string(&self.page_bytes).as_str())?;
        }
        if !self.after.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("after", pbjson::private::base64::encode(&self.after).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MultiGetRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table",
            "keys",
            "selection",
            "page_bytes",
            "pageBytes",
            "after",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Table,
            Keys,
            Selection,
            PageBytes,
            After,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "table" => Ok(GeneratedField::Table),
                            "keys" => Ok(GeneratedField::Keys),
                            "selection" => Ok(GeneratedField::Selection),
                            "pageBytes" | "page_bytes" => Ok(GeneratedField::PageBytes),
                            "after" => Ok(GeneratedField::After),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MultiGetRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MultiGetRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MultiGetRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table__ = None;
                let mut keys__ = None;
                let mut selection__ = None;
                let mut page_bytes__ = None;
                let mut after__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Keys => {
                            if keys__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keys"));
                            }
                            keys__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::Selection => {
                            if selection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("selection"));
                            }
                            selection__ = map_.next_value()?;
                        }
                        GeneratedField::PageBytes => {
                            if page_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pageBytes"));
                            }
                            page_bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::After => {
                            if after__.is_some() {
                                return Err(serde::de::Error::duplicate_field("after"));
                            }
                            after__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(MultiGetRequest {
                    table: table__.unwrap_or_default(),
                    keys: keys__.unwrap_or_default(),
                    selection: selection__,
                    page_bytes: page_bytes__.unwrap_or_default(),
                    after: after__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MultiGetRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MultiGetResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.rows.is_empty() {
            len += 1;
        }
        if !self.next.is_empty() {
            len += 1;
        }
        if self.bytes != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MultiGetResponse", len)?;
        if !self.rows.is_empty() {
            struct_ser.serialize_field("rows", &self.rows)?;
        }
        if !self.next.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("next", pbjson::private::base64::encode(&self.next).as_str())?;
        }
        if self.bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("bytes", ToString::to_string(&self.bytes).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MultiGetResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "rows",
            "next",
            "bytes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Rows,
            Next,
            Bytes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "rows" => Ok(GeneratedField::Rows),
                            "next" => Ok(GeneratedField::Next),
                            "bytes" => Ok(GeneratedField::Bytes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MultiGetResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MultiGetResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MultiGetResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut rows__ = None;
                let mut next__ = None;
                let mut bytes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Rows => {
                            if rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rows"));
                            }
                            rows__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Next => {
                            if next__.is_some() {
                                return Err(serde::de::Error::duplicate_field("next"));
                            }
                            next__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Bytes => {
                            if bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bytes"));
                            }
                            bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(MultiGetResponse {
                    rows: rows__.unwrap_or_default(),
                    next: next__.unwrap_or_default(),
                    bytes: bytes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MultiGetResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MutateRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.writes.is_some() {
            len += 1;
        }
        if self.durability != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MutateRequest", len)?;
        if let Some(v) = self.writes.as_ref() {
            struct_ser.serialize_field("writes", v)?;
        }
        if self.durability != 0 {
            let v = Durability::try_from(self.durability)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.durability)))?;
            struct_ser.serialize_field("durability", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MutateRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "writes",
            "durability",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Writes,
            Durability,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "writes" => Ok(GeneratedField::Writes),
                            "durability" => Ok(GeneratedField::Durability),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MutateRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MutateRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MutateRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut writes__ = None;
                let mut durability__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Writes => {
                            if writes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("writes"));
                            }
                            writes__ = map_.next_value()?;
                        }
                        GeneratedField::Durability => {
                            if durability__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durability"));
                            }
                            durability__ = Some(map_.next_value::<Durability>()? as i32);
                        }
                    }
                }
                Ok(MutateRequest {
                    writes: writes__,
                    durability: durability__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MutateRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MutateResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.commit.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MutateResponse", len)?;
        if let Some(v) = self.commit.as_ref() {
            struct_ser.serialize_field("commit", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MutateResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "commit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Commit,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "commit" => Ok(GeneratedField::Commit),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MutateResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MutateResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MutateResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut commit__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Commit => {
                            if commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("commit"));
                            }
                            commit__ = map_.next_value()?;
                        }
                    }
                }
                Ok(MutateResponse {
                    commit: commit__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MutateResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MutateReturningResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.commit.is_some() {
            len += 1;
        }
        if !self.cells.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.MutateReturningResponse", len)?;
        if let Some(v) = self.commit.as_ref() {
            struct_ser.serialize_field("commit", v)?;
        }
        if !self.cells.is_empty() {
            struct_ser.serialize_field("cells", &self.cells)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MutateReturningResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "commit",
            "cells",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Commit,
            Cells,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "commit" => Ok(GeneratedField::Commit),
                            "cells" => Ok(GeneratedField::Cells),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MutateReturningResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.MutateReturningResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MutateReturningResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut commit__ = None;
                let mut cells__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Commit => {
                            if commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("commit"));
                            }
                            commit__ = map_.next_value()?;
                        }
                        GeneratedField::Cells => {
                            if cells__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cells"));
                            }
                            cells__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(MutateReturningResponse {
                    commit: commit__,
                    cells: cells__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.MutateReturningResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NearestRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table.is_empty() {
            len += 1;
        }
        if !self.family.is_empty() {
            len += 1;
        }
        if self.query.is_some() {
            len += 1;
        }
        if self.k != 0 {
            len += 1;
        }
        if !self.snapshot_id.is_empty() {
            len += 1;
        }
        if self.page_bytes != 0 {
            len += 1;
        }
        if self.source.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.NearestRequest", len)?;
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if let Some(v) = self.query.as_ref() {
            struct_ser.serialize_field("query", v)?;
        }
        if self.k != 0 {
            struct_ser.serialize_field("k", &self.k)?;
        }
        if !self.snapshot_id.is_empty() {
            struct_ser.serialize_field("snapshot_id", &self.snapshot_id)?;
        }
        if self.page_bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("page_bytes", ToString::to_string(&self.page_bytes).as_str())?;
        }
        if let Some(v) = self.source.as_ref() {
            match v {
                nearest_request::Source::Range(v) => {
                    struct_ser.serialize_field("range", v)?;
                }
                nearest_request::Source::Keys(v) => {
                    struct_ser.serialize_field("keys", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NearestRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table",
            "family",
            "query",
            "k",
            "snapshot_id",
            "snapshotId",
            "page_bytes",
            "pageBytes",
            "range",
            "keys",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Table,
            Family,
            Query,
            K,
            SnapshotId,
            PageBytes,
            Range,
            Keys,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "table" => Ok(GeneratedField::Table),
                            "family" => Ok(GeneratedField::Family),
                            "query" => Ok(GeneratedField::Query),
                            "k" => Ok(GeneratedField::K),
                            "snapshotId" | "snapshot_id" => Ok(GeneratedField::SnapshotId),
                            "pageBytes" | "page_bytes" => Ok(GeneratedField::PageBytes),
                            "range" => Ok(GeneratedField::Range),
                            "keys" => Ok(GeneratedField::Keys),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NearestRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.NearestRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NearestRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table__ = None;
                let mut family__ = None;
                let mut query__ = None;
                let mut k__ = None;
                let mut snapshot_id__ = None;
                let mut page_bytes__ = None;
                let mut source__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Query => {
                            if query__.is_some() {
                                return Err(serde::de::Error::duplicate_field("query"));
                            }
                            query__ = map_.next_value()?;
                        }
                        GeneratedField::K => {
                            if k__.is_some() {
                                return Err(serde::de::Error::duplicate_field("k"));
                            }
                            k__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SnapshotId => {
                            if snapshot_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotId"));
                            }
                            snapshot_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::PageBytes => {
                            if page_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pageBytes"));
                            }
                            page_bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Range => {
                            if source__.is_some() {
                                return Err(serde::de::Error::duplicate_field("range"));
                            }
                            source__ = map_.next_value::<::std::option::Option<_>>()?.map(nearest_request::Source::Range)
;
                        }
                        GeneratedField::Keys => {
                            if source__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keys"));
                            }
                            source__ = map_.next_value::<::std::option::Option<_>>()?.map(nearest_request::Source::Keys)
;
                        }
                    }
                }
                Ok(NearestRequest {
                    table: table__.unwrap_or_default(),
                    family: family__.unwrap_or_default(),
                    query: query__,
                    k: k__.unwrap_or_default(),
                    snapshot_id: snapshot_id__.unwrap_or_default(),
                    page_bytes: page_bytes__.unwrap_or_default(),
                    source: source__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.NearestRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NearestResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.neighbours.is_empty() {
            len += 1;
        }
        if self.cut_by_budget {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.NearestResponse", len)?;
        if !self.neighbours.is_empty() {
            struct_ser.serialize_field("neighbours", &self.neighbours)?;
        }
        if self.cut_by_budget {
            struct_ser.serialize_field("cut_by_budget", &self.cut_by_budget)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NearestResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "neighbours",
            "cut_by_budget",
            "cutByBudget",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Neighbours,
            CutByBudget,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "neighbours" => Ok(GeneratedField::Neighbours),
                            "cutByBudget" | "cut_by_budget" => Ok(GeneratedField::CutByBudget),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NearestResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.NearestResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NearestResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut neighbours__ = None;
                let mut cut_by_budget__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Neighbours => {
                            if neighbours__.is_some() {
                                return Err(serde::de::Error::duplicate_field("neighbours"));
                            }
                            neighbours__ = Some(map_.next_value()?);
                        }
                        GeneratedField::CutByBudget => {
                            if cut_by_budget__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cutByBudget"));
                            }
                            cut_by_budget__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(NearestResponse {
                    neighbours: neighbours__.unwrap_or_default(),
                    cut_by_budget: cut_by_budget__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.NearestResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Neighbour {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.row.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        if self.distance != 0. {
            len += 1;
        }
        if self.timestamp != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Neighbour", len)?;
        if !self.row.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("row", pbjson::private::base64::encode(&self.row).as_str())?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        if self.distance != 0. {
            struct_ser.serialize_field("distance", &self.distance)?;
        }
        if self.timestamp != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("timestamp", ToString::to_string(&self.timestamp).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Neighbour {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "row",
            "qualifier",
            "distance",
            "timestamp",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Row,
            Qualifier,
            Distance,
            Timestamp,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "row" => Ok(GeneratedField::Row),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            "distance" => Ok(GeneratedField::Distance),
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Neighbour;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Neighbour")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Neighbour, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut row__ = None;
                let mut qualifier__ = None;
                let mut distance__ = None;
                let mut timestamp__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Row => {
                            if row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("row"));
                            }
                            row__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Distance => {
                            if distance__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distance"));
                            }
                            distance__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Neighbour {
                    row: row__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                    distance: distance__.unwrap_or_default(),
                    timestamp: timestamp__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Neighbour", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NewMemory {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.text.is_empty() {
            len += 1;
        }
        if self.embedding.is_some() {
            len += 1;
        }
        if self.kind != 0 {
            len += 1;
        }
        if self.importance.is_some() {
            len += 1;
        }
        if !self.source.is_empty() {
            len += 1;
        }
        if !self.session.is_empty() {
            len += 1;
        }
        if self.pinned {
            len += 1;
        }
        if !self.links.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.NewMemory", len)?;
        if !self.text.is_empty() {
            struct_ser.serialize_field("text", &self.text)?;
        }
        if let Some(v) = self.embedding.as_ref() {
            struct_ser.serialize_field("embedding", v)?;
        }
        if self.kind != 0 {
            let v = MemoryKind::try_from(self.kind)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.kind)))?;
            struct_ser.serialize_field("kind", &v)?;
        }
        if let Some(v) = self.importance.as_ref() {
            struct_ser.serialize_field("importance", v)?;
        }
        if !self.source.is_empty() {
            struct_ser.serialize_field("source", &self.source)?;
        }
        if !self.session.is_empty() {
            struct_ser.serialize_field("session", &self.session)?;
        }
        if self.pinned {
            struct_ser.serialize_field("pinned", &self.pinned)?;
        }
        if !self.links.is_empty() {
            struct_ser.serialize_field("links", &self.links)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NewMemory {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "text",
            "embedding",
            "kind",
            "importance",
            "source",
            "session",
            "pinned",
            "links",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Text,
            Embedding,
            Kind,
            Importance,
            Source,
            Session,
            Pinned,
            Links,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "text" => Ok(GeneratedField::Text),
                            "embedding" => Ok(GeneratedField::Embedding),
                            "kind" => Ok(GeneratedField::Kind),
                            "importance" => Ok(GeneratedField::Importance),
                            "source" => Ok(GeneratedField::Source),
                            "session" => Ok(GeneratedField::Session),
                            "pinned" => Ok(GeneratedField::Pinned),
                            "links" => Ok(GeneratedField::Links),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NewMemory;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.NewMemory")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NewMemory, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut text__ = None;
                let mut embedding__ = None;
                let mut kind__ = None;
                let mut importance__ = None;
                let mut source__ = None;
                let mut session__ = None;
                let mut pinned__ = None;
                let mut links__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Text => {
                            if text__.is_some() {
                                return Err(serde::de::Error::duplicate_field("text"));
                            }
                            text__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Embedding => {
                            if embedding__.is_some() {
                                return Err(serde::de::Error::duplicate_field("embedding"));
                            }
                            embedding__ = map_.next_value()?;
                        }
                        GeneratedField::Kind => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("kind"));
                            }
                            kind__ = Some(map_.next_value::<MemoryKind>()? as i32);
                        }
                        GeneratedField::Importance => {
                            if importance__.is_some() {
                                return Err(serde::de::Error::duplicate_field("importance"));
                            }
                            importance__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Source => {
                            if source__.is_some() {
                                return Err(serde::de::Error::duplicate_field("source"));
                            }
                            source__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Session => {
                            if session__.is_some() {
                                return Err(serde::de::Error::duplicate_field("session"));
                            }
                            session__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Pinned => {
                            if pinned__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pinned"));
                            }
                            pinned__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Links => {
                            if links__.is_some() {
                                return Err(serde::de::Error::duplicate_field("links"));
                            }
                            links__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(NewMemory {
                    text: text__.unwrap_or_default(),
                    embedding: embedding__,
                    kind: kind__.unwrap_or_default(),
                    importance: importance__,
                    source: source__.unwrap_or_default(),
                    session: session__.unwrap_or_default(),
                    pinned: pinned__.unwrap_or_default(),
                    links: links__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.NewMemory", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Normalization {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "NORMALIZATION_UNSPECIFIED",
            Self::Nfc => "NORMALIZATION_NFC",
            Self::Nfkc => "NORMALIZATION_NFKC",
            Self::None => "NORMALIZATION_NONE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Normalization {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NORMALIZATION_UNSPECIFIED",
            "NORMALIZATION_NFC",
            "NORMALIZATION_NFKC",
            "NORMALIZATION_NONE",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = Normalization;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "NORMALIZATION_UNSPECIFIED" => Ok(Normalization::Unspecified),
                    "NORMALIZATION_NFC" => Ok(Normalization::Nfc),
                    "NORMALIZATION_NFKC" => Ok(Normalization::Nfkc),
                    "NORMALIZATION_NONE" => Ok(Normalization::None),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for OpenSnapshotRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.durable {
            len += 1;
        }
        if self.route.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.OpenSnapshotRequest", len)?;
        if self.durable {
            struct_ser.serialize_field("durable", &self.durable)?;
        }
        if let Some(v) = self.route.as_ref() {
            struct_ser.serialize_field("route", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OpenSnapshotRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "durable",
            "route",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Durable,
            Route,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "durable" => Ok(GeneratedField::Durable),
                            "route" => Ok(GeneratedField::Route),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OpenSnapshotRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.OpenSnapshotRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<OpenSnapshotRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut durable__ = None;
                let mut route__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Durable => {
                            if durable__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durable"));
                            }
                            durable__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Route => {
                            if route__.is_some() {
                                return Err(serde::de::Error::duplicate_field("route"));
                            }
                            route__ = map_.next_value()?;
                        }
                    }
                }
                Ok(OpenSnapshotRequest {
                    durable: durable__.unwrap_or_default(),
                    route: route__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.OpenSnapshotRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OpenSnapshotResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.snapshot_id.is_empty() {
            len += 1;
        }
        if self.seqno != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.OpenSnapshotResponse", len)?;
        if !self.snapshot_id.is_empty() {
            struct_ser.serialize_field("snapshot_id", &self.snapshot_id)?;
        }
        if self.seqno != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("seqno", ToString::to_string(&self.seqno).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OpenSnapshotResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "snapshot_id",
            "snapshotId",
            "seqno",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SnapshotId,
            Seqno,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "snapshotId" | "snapshot_id" => Ok(GeneratedField::SnapshotId),
                            "seqno" => Ok(GeneratedField::Seqno),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OpenSnapshotResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.OpenSnapshotResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<OpenSnapshotResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut snapshot_id__ = None;
                let mut seqno__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::SnapshotId => {
                            if snapshot_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotId"));
                            }
                            snapshot_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Seqno => {
                            if seqno__.is_some() {
                                return Err(serde::de::Error::duplicate_field("seqno"));
                            }
                            seqno__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(OpenSnapshotResponse {
                    snapshot_id: snapshot_id__.unwrap_or_default(),
                    seqno: seqno__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.OpenSnapshotResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Ordering {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "ORDERING_UNSPECIFIED",
            Self::Less => "ORDERING_LESS",
            Self::Equal => "ORDERING_EQUAL",
            Self::Greater => "ORDERING_GREATER",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Ordering {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ORDERING_UNSPECIFIED",
            "ORDERING_LESS",
            "ORDERING_EQUAL",
            "ORDERING_GREATER",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = Ordering;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "ORDERING_UNSPECIFIED" => Ok(Ordering::Unspecified),
                    "ORDERING_LESS" => Ok(Ordering::Less),
                    "ORDERING_EQUAL" => Ok(Ordering::Equal),
                    "ORDERING_GREATER" => Ok(Ordering::Greater),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Phrase {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.text.is_empty() {
            len += 1;
        }
        if self.slop != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Phrase", len)?;
        if !self.text.is_empty() {
            struct_ser.serialize_field("text", &self.text)?;
        }
        if self.slop != 0 {
            struct_ser.serialize_field("slop", &self.slop)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Phrase {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "text",
            "slop",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Text,
            Slop,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "text" => Ok(GeneratedField::Text),
                            "slop" => Ok(GeneratedField::Slop),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Phrase;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Phrase")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Phrase, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut text__ = None;
                let mut slop__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Text => {
                            if text__.is_some() {
                                return Err(serde::de::Error::duplicate_field("text"));
                            }
                            text__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Slop => {
                            if slop__.is_some() {
                                return Err(serde::de::Error::duplicate_field("slop"));
                            }
                            slop__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Phrase {
                    text: text__.unwrap_or_default(),
                    slop: slop__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Phrase", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Priority {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "PRIORITY_UNSPECIFIED",
            Self::Low => "PRIORITY_LOW",
            Self::Normal => "PRIORITY_NORMAL",
            Self::High => "PRIORITY_HIGH",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Priority {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "PRIORITY_UNSPECIFIED",
            "PRIORITY_LOW",
            "PRIORITY_NORMAL",
            "PRIORITY_HIGH",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = Priority;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "PRIORITY_UNSPECIFIED" => Ok(Priority::Unspecified),
                    "PRIORITY_LOW" => Ok(Priority::Low),
                    "PRIORITY_NORMAL" => Ok(Priority::Normal),
                    "PRIORITY_HIGH" => Ok(Priority::High),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Progress {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("pigeonhole.v1.Progress", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Progress {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Progress;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Progress")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Progress, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(Progress {
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Progress", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Put {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.family.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        if self.timestamp.is_some() {
            len += 1;
        }
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Put", len)?;
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        if let Some(v) = self.timestamp.as_ref() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("timestamp", ToString::to_string(&v).as_str())?;
        }
        if let Some(v) = self.value.as_ref() {
            struct_ser.serialize_field("value", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Put {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "family",
            "qualifier",
            "timestamp",
            "value",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Family,
            Qualifier,
            Timestamp,
            Value,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "family" => Ok(GeneratedField::Family),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            "value" => Ok(GeneratedField::Value),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Put;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Put")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Put, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut family__ = None;
                let mut qualifier__ = None;
                let mut timestamp__ = None;
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Put {
                    family: family__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                    timestamp: timestamp__,
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Put", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for QuantileReading {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.count != 0 {
            len += 1;
        }
        if !self.values.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.QuantileReading", len)?;
        if self.count != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("count", ToString::to_string(&self.count).as_str())?;
        }
        if !self.values.is_empty() {
            struct_ser.serialize_field("values", &self.values)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for QuantileReading {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "count",
            "values",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Count,
            Values,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "count" => Ok(GeneratedField::Count),
                            "values" => Ok(GeneratedField::Values),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = QuantileReading;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.QuantileReading")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<QuantileReading, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut count__ = None;
                let mut values__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Count => {
                            if count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("count"));
                            }
                            count__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Values => {
                            if values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(QuantileReading {
                    count: count__.unwrap_or_default(),
                    values: values__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.QuantileReading", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReadRowRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table.is_empty() {
            len += 1;
        }
        if !self.row.is_empty() {
            len += 1;
        }
        if self.selection.is_some() {
            len += 1;
        }
        if self.page_cells != 0 {
            len += 1;
        }
        if !self.after.is_empty() {
            len += 1;
        }
        if self.page_bytes != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ReadRowRequest", len)?;
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        if !self.row.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("row", pbjson::private::base64::encode(&self.row).as_str())?;
        }
        if let Some(v) = self.selection.as_ref() {
            struct_ser.serialize_field("selection", v)?;
        }
        if self.page_cells != 0 {
            struct_ser.serialize_field("page_cells", &self.page_cells)?;
        }
        if !self.after.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("after", pbjson::private::base64::encode(&self.after).as_str())?;
        }
        if self.page_bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("page_bytes", ToString::to_string(&self.page_bytes).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReadRowRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table",
            "row",
            "selection",
            "page_cells",
            "pageCells",
            "after",
            "page_bytes",
            "pageBytes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Table,
            Row,
            Selection,
            PageCells,
            After,
            PageBytes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "table" => Ok(GeneratedField::Table),
                            "row" => Ok(GeneratedField::Row),
                            "selection" => Ok(GeneratedField::Selection),
                            "pageCells" | "page_cells" => Ok(GeneratedField::PageCells),
                            "after" => Ok(GeneratedField::After),
                            "pageBytes" | "page_bytes" => Ok(GeneratedField::PageBytes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReadRowRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ReadRowRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ReadRowRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table__ = None;
                let mut row__ = None;
                let mut selection__ = None;
                let mut page_cells__ = None;
                let mut after__ = None;
                let mut page_bytes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Row => {
                            if row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("row"));
                            }
                            row__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Selection => {
                            if selection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("selection"));
                            }
                            selection__ = map_.next_value()?;
                        }
                        GeneratedField::PageCells => {
                            if page_cells__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pageCells"));
                            }
                            page_cells__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::After => {
                            if after__.is_some() {
                                return Err(serde::de::Error::duplicate_field("after"));
                            }
                            after__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PageBytes => {
                            if page_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pageBytes"));
                            }
                            page_bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ReadRowRequest {
                    table: table__.unwrap_or_default(),
                    row: row__.unwrap_or_default(),
                    selection: selection__,
                    page_cells: page_cells__.unwrap_or_default(),
                    after: after__.unwrap_or_default(),
                    page_bytes: page_bytes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ReadRowRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReadRowResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.row.is_some() {
            len += 1;
        }
        if !self.next.is_empty() {
            len += 1;
        }
        if self.bytes != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ReadRowResponse", len)?;
        if let Some(v) = self.row.as_ref() {
            struct_ser.serialize_field("row", v)?;
        }
        if !self.next.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("next", pbjson::private::base64::encode(&self.next).as_str())?;
        }
        if self.bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("bytes", ToString::to_string(&self.bytes).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReadRowResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "row",
            "next",
            "bytes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Row,
            Next,
            Bytes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "row" => Ok(GeneratedField::Row),
                            "next" => Ok(GeneratedField::Next),
                            "bytes" => Ok(GeneratedField::Bytes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReadRowResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ReadRowResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ReadRowResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut row__ = None;
                let mut next__ = None;
                let mut bytes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Row => {
                            if row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("row"));
                            }
                            row__ = map_.next_value()?;
                        }
                        GeneratedField::Next => {
                            if next__.is_some() {
                                return Err(serde::de::Error::duplicate_field("next"));
                            }
                            next__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Bytes => {
                            if bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bytes"));
                            }
                            bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ReadRowResponse {
                    row: row__,
                    next: next__.unwrap_or_default(),
                    bytes: bytes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ReadRowResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Rebalance {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "REBALANCE_UNSPECIFIED",
            Self::Inline => "REBALANCE_INLINE",
            Self::Background => "REBALANCE_BACKGROUND",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Rebalance {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "REBALANCE_UNSPECIFIED",
            "REBALANCE_INLINE",
            "REBALANCE_BACKGROUND",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = Rebalance;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "REBALANCE_UNSPECIFIED" => Ok(Rebalance::Unspecified),
                    "REBALANCE_INLINE" => Ok(Rebalance::Inline),
                    "REBALANCE_BACKGROUND" => Ok(Rebalance::Background),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for RecallRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.namespaces.is_empty() {
            len += 1;
        }
        if !self.text.is_empty() {
            len += 1;
        }
        if self.embedding.is_some() {
            len += 1;
        }
        if self.k != 0 {
            len += 1;
        }
        if self.byte_budget != 0 {
            len += 1;
        }
        if self.expand_hops != 0 {
            len += 1;
        }
        if self.exact {
            len += 1;
        }
        if self.include_superseded {
            len += 1;
        }
        if !self.session.is_empty() {
            len += 1;
        }
        if self.with_embeddings {
            len += 1;
        }
        if self.with_signals {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.RecallRequest", len)?;
        if !self.namespaces.is_empty() {
            struct_ser.serialize_field("namespaces", &self.namespaces)?;
        }
        if !self.text.is_empty() {
            struct_ser.serialize_field("text", &self.text)?;
        }
        if let Some(v) = self.embedding.as_ref() {
            struct_ser.serialize_field("embedding", v)?;
        }
        if self.k != 0 {
            struct_ser.serialize_field("k", &self.k)?;
        }
        if self.byte_budget != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("byte_budget", ToString::to_string(&self.byte_budget).as_str())?;
        }
        if self.expand_hops != 0 {
            struct_ser.serialize_field("expand_hops", &self.expand_hops)?;
        }
        if self.exact {
            struct_ser.serialize_field("exact", &self.exact)?;
        }
        if self.include_superseded {
            struct_ser.serialize_field("include_superseded", &self.include_superseded)?;
        }
        if !self.session.is_empty() {
            struct_ser.serialize_field("session", &self.session)?;
        }
        if self.with_embeddings {
            struct_ser.serialize_field("with_embeddings", &self.with_embeddings)?;
        }
        if self.with_signals {
            struct_ser.serialize_field("with_signals", &self.with_signals)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RecallRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "namespaces",
            "text",
            "embedding",
            "k",
            "byte_budget",
            "byteBudget",
            "expand_hops",
            "expandHops",
            "exact",
            "include_superseded",
            "includeSuperseded",
            "session",
            "with_embeddings",
            "withEmbeddings",
            "with_signals",
            "withSignals",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Namespaces,
            Text,
            Embedding,
            K,
            ByteBudget,
            ExpandHops,
            Exact,
            IncludeSuperseded,
            Session,
            WithEmbeddings,
            WithSignals,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "namespaces" => Ok(GeneratedField::Namespaces),
                            "text" => Ok(GeneratedField::Text),
                            "embedding" => Ok(GeneratedField::Embedding),
                            "k" => Ok(GeneratedField::K),
                            "byteBudget" | "byte_budget" => Ok(GeneratedField::ByteBudget),
                            "expandHops" | "expand_hops" => Ok(GeneratedField::ExpandHops),
                            "exact" => Ok(GeneratedField::Exact),
                            "includeSuperseded" | "include_superseded" => Ok(GeneratedField::IncludeSuperseded),
                            "session" => Ok(GeneratedField::Session),
                            "withEmbeddings" | "with_embeddings" => Ok(GeneratedField::WithEmbeddings),
                            "withSignals" | "with_signals" => Ok(GeneratedField::WithSignals),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RecallRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.RecallRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RecallRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut namespaces__ = None;
                let mut text__ = None;
                let mut embedding__ = None;
                let mut k__ = None;
                let mut byte_budget__ = None;
                let mut expand_hops__ = None;
                let mut exact__ = None;
                let mut include_superseded__ = None;
                let mut session__ = None;
                let mut with_embeddings__ = None;
                let mut with_signals__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Namespaces => {
                            if namespaces__.is_some() {
                                return Err(serde::de::Error::duplicate_field("namespaces"));
                            }
                            namespaces__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Text => {
                            if text__.is_some() {
                                return Err(serde::de::Error::duplicate_field("text"));
                            }
                            text__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Embedding => {
                            if embedding__.is_some() {
                                return Err(serde::de::Error::duplicate_field("embedding"));
                            }
                            embedding__ = map_.next_value()?;
                        }
                        GeneratedField::K => {
                            if k__.is_some() {
                                return Err(serde::de::Error::duplicate_field("k"));
                            }
                            k__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ByteBudget => {
                            if byte_budget__.is_some() {
                                return Err(serde::de::Error::duplicate_field("byteBudget"));
                            }
                            byte_budget__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ExpandHops => {
                            if expand_hops__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expandHops"));
                            }
                            expand_hops__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Exact => {
                            if exact__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exact"));
                            }
                            exact__ = Some(map_.next_value()?);
                        }
                        GeneratedField::IncludeSuperseded => {
                            if include_superseded__.is_some() {
                                return Err(serde::de::Error::duplicate_field("includeSuperseded"));
                            }
                            include_superseded__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Session => {
                            if session__.is_some() {
                                return Err(serde::de::Error::duplicate_field("session"));
                            }
                            session__ = Some(map_.next_value()?);
                        }
                        GeneratedField::WithEmbeddings => {
                            if with_embeddings__.is_some() {
                                return Err(serde::de::Error::duplicate_field("withEmbeddings"));
                            }
                            with_embeddings__ = Some(map_.next_value()?);
                        }
                        GeneratedField::WithSignals => {
                            if with_signals__.is_some() {
                                return Err(serde::de::Error::duplicate_field("withSignals"));
                            }
                            with_signals__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(RecallRequest {
                    namespaces: namespaces__.unwrap_or_default(),
                    text: text__.unwrap_or_default(),
                    embedding: embedding__,
                    k: k__.unwrap_or_default(),
                    byte_budget: byte_budget__.unwrap_or_default(),
                    expand_hops: expand_hops__.unwrap_or_default(),
                    exact: exact__.unwrap_or_default(),
                    include_superseded: include_superseded__.unwrap_or_default(),
                    session: session__.unwrap_or_default(),
                    with_embeddings: with_embeddings__.unwrap_or_default(),
                    with_signals: with_signals__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.RecallRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RecallResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.hits.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.RecallResponse", len)?;
        if !self.hits.is_empty() {
            struct_ser.serialize_field("hits", &self.hits)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RecallResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hits",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Hits,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "hits" => Ok(GeneratedField::Hits),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RecallResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.RecallResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RecallResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hits__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Hits => {
                            if hits__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hits"));
                            }
                            hits__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(RecallResponse {
                    hits: hits__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.RecallResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReleaseSnapshotRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.snapshot_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ReleaseSnapshotRequest", len)?;
        if !self.snapshot_id.is_empty() {
            struct_ser.serialize_field("snapshot_id", &self.snapshot_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReleaseSnapshotRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "snapshot_id",
            "snapshotId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SnapshotId,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "snapshotId" | "snapshot_id" => Ok(GeneratedField::SnapshotId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReleaseSnapshotRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ReleaseSnapshotRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ReleaseSnapshotRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut snapshot_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::SnapshotId => {
                            if snapshot_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotId"));
                            }
                            snapshot_id__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ReleaseSnapshotRequest {
                    snapshot_id: snapshot_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ReleaseSnapshotRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RememberOutcome {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "REMEMBER_OUTCOME_UNSPECIFIED",
            Self::New => "REMEMBER_OUTCOME_NEW",
            Self::Merged => "REMEMBER_OUTCOME_MERGED",
            Self::Similar => "REMEMBER_OUTCOME_SIMILAR",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for RememberOutcome {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "REMEMBER_OUTCOME_UNSPECIFIED",
            "REMEMBER_OUTCOME_NEW",
            "REMEMBER_OUTCOME_MERGED",
            "REMEMBER_OUTCOME_SIMILAR",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = RememberOutcome;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "REMEMBER_OUTCOME_UNSPECIFIED" => Ok(RememberOutcome::Unspecified),
                    "REMEMBER_OUTCOME_NEW" => Ok(RememberOutcome::New),
                    "REMEMBER_OUTCOME_MERGED" => Ok(RememberOutcome::Merged),
                    "REMEMBER_OUTCOME_SIMILAR" => Ok(RememberOutcome::Similar),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for RememberRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.namespace.is_empty() {
            len += 1;
        }
        if self.memory.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.RememberRequest", len)?;
        if !self.namespace.is_empty() {
            struct_ser.serialize_field("namespace", &self.namespace)?;
        }
        if let Some(v) = self.memory.as_ref() {
            struct_ser.serialize_field("memory", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RememberRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "namespace",
            "memory",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Namespace,
            Memory,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "namespace" => Ok(GeneratedField::Namespace),
                            "memory" => Ok(GeneratedField::Memory),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RememberRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.RememberRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RememberRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut namespace__ = None;
                let mut memory__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Namespace => {
                            if namespace__.is_some() {
                                return Err(serde::de::Error::duplicate_field("namespace"));
                            }
                            namespace__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Memory => {
                            if memory__.is_some() {
                                return Err(serde::de::Error::duplicate_field("memory"));
                            }
                            memory__ = map_.next_value()?;
                        }
                    }
                }
                Ok(RememberRequest {
                    namespace: namespace__.unwrap_or_default(),
                    memory: memory__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.RememberRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RememberResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if self.outcome != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.RememberResponse", len)?;
        if !self.id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id", pbjson::private::base64::encode(&self.id).as_str())?;
        }
        if self.outcome != 0 {
            let v = RememberOutcome::try_from(self.outcome)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.outcome)))?;
            struct_ser.serialize_field("outcome", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RememberResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "outcome",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Outcome,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "outcome" => Ok(GeneratedField::Outcome),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RememberResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.RememberResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RememberResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut outcome__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Outcome => {
                            if outcome__.is_some() {
                                return Err(serde::de::Error::duplicate_field("outcome"));
                            }
                            outcome__ = Some(map_.next_value::<RememberOutcome>()? as i32);
                        }
                    }
                }
                Ok(RememberResponse {
                    id: id__.unwrap_or_default(),
                    outcome: outcome__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.RememberResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RetryFrom {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "RETRY_FROM_UNSPECIFIED",
            Self::Request => "RETRY_FROM_REQUEST",
            Self::Begin => "RETRY_FROM_BEGIN",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for RetryFrom {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "RETRY_FROM_UNSPECIFIED",
            "RETRY_FROM_REQUEST",
            "RETRY_FROM_BEGIN",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = RetryFrom;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "RETRY_FROM_UNSPECIFIED" => Ok(RetryFrom::Unspecified),
                    "RETRY_FROM_REQUEST" => Ok(RetryFrom::Request),
                    "RETRY_FROM_BEGIN" => Ok(RetryFrom::Begin),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for ReturnedCell {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.family.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        if self.timestamp != 0 {
            len += 1;
        }
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ReturnedCell", len)?;
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        if self.timestamp != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("timestamp", ToString::to_string(&self.timestamp).as_str())?;
        }
        if let Some(v) = self.value.as_ref() {
            match v {
                returned_cell::Value::I64(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("i64", ToString::to_string(&v).as_str())?;
                }
                returned_cell::Value::Absent(v) => {
                    struct_ser.serialize_field("absent", v)?;
                }
                returned_cell::Value::Error(v) => {
                    struct_ser.serialize_field("error", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReturnedCell {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "family",
            "qualifier",
            "timestamp",
            "i64",
            "absent",
            "error",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Family,
            Qualifier,
            Timestamp,
            I64,
            Absent,
            Error,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "family" => Ok(GeneratedField::Family),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            "i64" => Ok(GeneratedField::I64),
                            "absent" => Ok(GeneratedField::Absent),
                            "error" => Ok(GeneratedField::Error),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReturnedCell;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ReturnedCell")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ReturnedCell, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut family__ = None;
                let mut qualifier__ = None;
                let mut timestamp__ = None;
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::I64 => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("i64"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| returned_cell::Value::I64(x.0));
                        }
                        GeneratedField::Absent => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("absent"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(returned_cell::Value::Absent)
;
                        }
                        GeneratedField::Error => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("error"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(returned_cell::Value::Error)
;
                        }
                    }
                }
                Ok(ReturnedCell {
                    family: family__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                    timestamp: timestamp__.unwrap_or_default(),
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ReturnedCell", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RoutingKey {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.key.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.RoutingKey", len)?;
        if let Some(v) = self.key.as_ref() {
            match v {
                routing_key::Key::Row(v) => {
                    struct_ser.serialize_field("row", v)?;
                }
                routing_key::Key::Partition(v) => {
                    struct_ser.serialize_field("partition", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RoutingKey {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "row",
            "partition",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Row,
            Partition,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "row" => Ok(GeneratedField::Row),
                            "partition" => Ok(GeneratedField::Partition),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RoutingKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.RoutingKey")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RoutingKey, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut key__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Row => {
                            if key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("row"));
                            }
                            key__ = map_.next_value::<::std::option::Option<_>>()?.map(routing_key::Key::Row)
;
                        }
                        GeneratedField::Partition => {
                            if key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partition"));
                            }
                            key__ = map_.next_value::<::std::option::Option<_>>()?.map(routing_key::Key::Partition);
                        }
                    }
                }
                Ok(RoutingKey {
                    key: key__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.RoutingKey", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Row {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.key.is_empty() {
            len += 1;
        }
        if !self.cells.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Row", len)?;
        if !self.key.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("key", pbjson::private::base64::encode(&self.key).as_str())?;
        }
        if !self.cells.is_empty() {
            struct_ser.serialize_field("cells", &self.cells)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Row {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "key",
            "cells",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Key,
            Cells,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "key" => Ok(GeneratedField::Key),
                            "cells" => Ok(GeneratedField::Cells),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Row;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Row")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Row, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut key__ = None;
                let mut cells__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Key => {
                            if key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Cells => {
                            if cells__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cells"));
                            }
                            cells__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Row {
                    key: key__.unwrap_or_default(),
                    cells: cells__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Row", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RowKey {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table.is_empty() {
            len += 1;
        }
        if !self.row.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.RowKey", len)?;
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        if !self.row.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("row", pbjson::private::base64::encode(&self.row).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RowKey {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table",
            "row",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Table,
            Row,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "table" => Ok(GeneratedField::Table),
                            "row" => Ok(GeneratedField::Row),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RowKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.RowKey")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RowKey, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table__ = None;
                let mut row__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Row => {
                            if row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("row"));
                            }
                            row__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(RowKey {
                    table: table__.unwrap_or_default(),
                    row: row__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.RowKey", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RowWrites {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.row.is_some() {
            len += 1;
        }
        if !self.writes.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.RowWrites", len)?;
        if let Some(v) = self.row.as_ref() {
            struct_ser.serialize_field("row", v)?;
        }
        if !self.writes.is_empty() {
            struct_ser.serialize_field("writes", &self.writes)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RowWrites {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "row",
            "writes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Row,
            Writes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "row" => Ok(GeneratedField::Row),
                            "writes" => Ok(GeneratedField::Writes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RowWrites;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.RowWrites")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RowWrites, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut row__ = None;
                let mut writes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Row => {
                            if row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("row"));
                            }
                            row__ = map_.next_value()?;
                        }
                        GeneratedField::Writes => {
                            if writes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("writes"));
                            }
                            writes__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(RowWrites {
                    row: row__,
                    writes: writes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.RowWrites", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScanPage {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.rows.is_empty() {
            len += 1;
        }
        if !self.next.is_empty() {
            len += 1;
        }
        if self.bytes != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ScanPage", len)?;
        if !self.rows.is_empty() {
            struct_ser.serialize_field("rows", &self.rows)?;
        }
        if !self.next.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("next", pbjson::private::base64::encode(&self.next).as_str())?;
        }
        if self.bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("bytes", ToString::to_string(&self.bytes).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScanPage {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "rows",
            "next",
            "bytes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Rows,
            Next,
            Bytes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "rows" => Ok(GeneratedField::Rows),
                            "next" => Ok(GeneratedField::Next),
                            "bytes" => Ok(GeneratedField::Bytes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScanPage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ScanPage")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ScanPage, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut rows__ = None;
                let mut next__ = None;
                let mut bytes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Rows => {
                            if rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rows"));
                            }
                            rows__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Next => {
                            if next__.is_some() {
                                return Err(serde::de::Error::duplicate_field("next"));
                            }
                            next__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Bytes => {
                            if bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bytes"));
                            }
                            bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ScanPage {
                    rows: rows__.unwrap_or_default(),
                    next: next__.unwrap_or_default(),
                    bytes: bytes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ScanPage", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScanRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table.is_empty() {
            len += 1;
        }
        if self.range.is_some() {
            len += 1;
        }
        if self.selection.is_some() {
            len += 1;
        }
        if self.limit != 0 {
            len += 1;
        }
        if self.page_bytes != 0 {
            len += 1;
        }
        if !self.after.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ScanRequest", len)?;
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        if let Some(v) = self.range.as_ref() {
            struct_ser.serialize_field("range", v)?;
        }
        if let Some(v) = self.selection.as_ref() {
            struct_ser.serialize_field("selection", v)?;
        }
        if self.limit != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("limit", ToString::to_string(&self.limit).as_str())?;
        }
        if self.page_bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("page_bytes", ToString::to_string(&self.page_bytes).as_str())?;
        }
        if !self.after.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("after", pbjson::private::base64::encode(&self.after).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScanRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table",
            "range",
            "selection",
            "limit",
            "page_bytes",
            "pageBytes",
            "after",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Table,
            Range,
            Selection,
            Limit,
            PageBytes,
            After,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "table" => Ok(GeneratedField::Table),
                            "range" => Ok(GeneratedField::Range),
                            "selection" => Ok(GeneratedField::Selection),
                            "limit" => Ok(GeneratedField::Limit),
                            "pageBytes" | "page_bytes" => Ok(GeneratedField::PageBytes),
                            "after" => Ok(GeneratedField::After),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScanRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ScanRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ScanRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table__ = None;
                let mut range__ = None;
                let mut selection__ = None;
                let mut limit__ = None;
                let mut page_bytes__ = None;
                let mut after__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Range => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("range"));
                            }
                            range__ = map_.next_value()?;
                        }
                        GeneratedField::Selection => {
                            if selection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("selection"));
                            }
                            selection__ = map_.next_value()?;
                        }
                        GeneratedField::Limit => {
                            if limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PageBytes => {
                            if page_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pageBytes"));
                            }
                            page_bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::After => {
                            if after__.is_some() {
                                return Err(serde::de::Error::duplicate_field("after"));
                            }
                            after__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ScanRequest {
                    table: table__.unwrap_or_default(),
                    range: range__,
                    selection: selection__,
                    limit: limit__.unwrap_or_default(),
                    page_bytes: page_bytes__.unwrap_or_default(),
                    after: after__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ScanRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Selection {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.families.is_empty() {
            len += 1;
        }
        if !self.qualifier_prefix.is_empty() {
            len += 1;
        }
        if self.qualifier_start.is_some() {
            len += 1;
        }
        if self.qualifier_end.is_some() {
            len += 1;
        }
        if self.versions != 0 {
            len += 1;
        }
        if self.all_versions {
            len += 1;
        }
        if self.time_range.is_some() {
            len += 1;
        }
        if self.column_limit != 0 {
            len += 1;
        }
        if self.value_filter.is_some() {
            len += 1;
        }
        if !self.snapshot_id.is_empty() {
            len += 1;
        }
        if self.aggregate.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Selection", len)?;
        if !self.families.is_empty() {
            struct_ser.serialize_field("families", &self.families)?;
        }
        if !self.qualifier_prefix.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier_prefix", pbjson::private::base64::encode(&self.qualifier_prefix).as_str())?;
        }
        if let Some(v) = self.qualifier_start.as_ref() {
            struct_ser.serialize_field("qualifier_start", v)?;
        }
        if let Some(v) = self.qualifier_end.as_ref() {
            struct_ser.serialize_field("qualifier_end", v)?;
        }
        if self.versions != 0 {
            struct_ser.serialize_field("versions", &self.versions)?;
        }
        if self.all_versions {
            struct_ser.serialize_field("all_versions", &self.all_versions)?;
        }
        if let Some(v) = self.time_range.as_ref() {
            struct_ser.serialize_field("time_range", v)?;
        }
        if self.column_limit != 0 {
            struct_ser.serialize_field("column_limit", &self.column_limit)?;
        }
        if let Some(v) = self.value_filter.as_ref() {
            struct_ser.serialize_field("value_filter", v)?;
        }
        if !self.snapshot_id.is_empty() {
            struct_ser.serialize_field("snapshot_id", &self.snapshot_id)?;
        }
        if let Some(v) = self.aggregate.as_ref() {
            struct_ser.serialize_field("aggregate", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Selection {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "families",
            "qualifier_prefix",
            "qualifierPrefix",
            "qualifier_start",
            "qualifierStart",
            "qualifier_end",
            "qualifierEnd",
            "versions",
            "all_versions",
            "allVersions",
            "time_range",
            "timeRange",
            "column_limit",
            "columnLimit",
            "value_filter",
            "valueFilter",
            "snapshot_id",
            "snapshotId",
            "aggregate",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Families,
            QualifierPrefix,
            QualifierStart,
            QualifierEnd,
            Versions,
            AllVersions,
            TimeRange,
            ColumnLimit,
            ValueFilter,
            SnapshotId,
            Aggregate,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "families" => Ok(GeneratedField::Families),
                            "qualifierPrefix" | "qualifier_prefix" => Ok(GeneratedField::QualifierPrefix),
                            "qualifierStart" | "qualifier_start" => Ok(GeneratedField::QualifierStart),
                            "qualifierEnd" | "qualifier_end" => Ok(GeneratedField::QualifierEnd),
                            "versions" => Ok(GeneratedField::Versions),
                            "allVersions" | "all_versions" => Ok(GeneratedField::AllVersions),
                            "timeRange" | "time_range" => Ok(GeneratedField::TimeRange),
                            "columnLimit" | "column_limit" => Ok(GeneratedField::ColumnLimit),
                            "valueFilter" | "value_filter" => Ok(GeneratedField::ValueFilter),
                            "snapshotId" | "snapshot_id" => Ok(GeneratedField::SnapshotId),
                            "aggregate" => Ok(GeneratedField::Aggregate),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Selection;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Selection")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Selection, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut families__ = None;
                let mut qualifier_prefix__ = None;
                let mut qualifier_start__ = None;
                let mut qualifier_end__ = None;
                let mut versions__ = None;
                let mut all_versions__ = None;
                let mut time_range__ = None;
                let mut column_limit__ = None;
                let mut value_filter__ = None;
                let mut snapshot_id__ = None;
                let mut aggregate__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Families => {
                            if families__.is_some() {
                                return Err(serde::de::Error::duplicate_field("families"));
                            }
                            families__ = Some(map_.next_value()?);
                        }
                        GeneratedField::QualifierPrefix => {
                            if qualifier_prefix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifierPrefix"));
                            }
                            qualifier_prefix__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::QualifierStart => {
                            if qualifier_start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifierStart"));
                            }
                            qualifier_start__ = map_.next_value()?;
                        }
                        GeneratedField::QualifierEnd => {
                            if qualifier_end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifierEnd"));
                            }
                            qualifier_end__ = map_.next_value()?;
                        }
                        GeneratedField::Versions => {
                            if versions__.is_some() {
                                return Err(serde::de::Error::duplicate_field("versions"));
                            }
                            versions__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::AllVersions => {
                            if all_versions__.is_some() {
                                return Err(serde::de::Error::duplicate_field("allVersions"));
                            }
                            all_versions__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TimeRange => {
                            if time_range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeRange"));
                            }
                            time_range__ = map_.next_value()?;
                        }
                        GeneratedField::ColumnLimit => {
                            if column_limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnLimit"));
                            }
                            column_limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ValueFilter => {
                            if value_filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("valueFilter"));
                            }
                            value_filter__ = map_.next_value()?;
                        }
                        GeneratedField::SnapshotId => {
                            if snapshot_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotId"));
                            }
                            snapshot_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Aggregate => {
                            if aggregate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregate"));
                            }
                            aggregate__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Selection {
                    families: families__.unwrap_or_default(),
                    qualifier_prefix: qualifier_prefix__.unwrap_or_default(),
                    qualifier_start: qualifier_start__,
                    qualifier_end: qualifier_end__,
                    versions: versions__.unwrap_or_default(),
                    all_versions: all_versions__.unwrap_or_default(),
                    time_range: time_range__,
                    column_limit: column_limit__.unwrap_or_default(),
                    value_filter: value_filter__,
                    snapshot_id: snapshot_id__.unwrap_or_default(),
                    aggregate: aggregate__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Selection", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SubscribeRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.tables.is_empty() {
            len += 1;
        }
        if !self.prefix.is_empty() {
            len += 1;
        }
        if self.visible {
            len += 1;
        }
        if self.start.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.SubscribeRequest", len)?;
        if !self.tables.is_empty() {
            struct_ser.serialize_field("tables", &self.tables)?;
        }
        if !self.prefix.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("prefix", pbjson::private::base64::encode(&self.prefix).as_str())?;
        }
        if self.visible {
            struct_ser.serialize_field("visible", &self.visible)?;
        }
        if let Some(v) = self.start.as_ref() {
            match v {
                subscribe_request::Start::FromSnapshot(v) => {
                    struct_ser.serialize_field("from_snapshot", v)?;
                }
                subscribe_request::Start::FromOldest(v) => {
                    struct_ser.serialize_field("from_oldest", v)?;
                }
                subscribe_request::Start::Resume(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("resume", pbjson::private::base64::encode(&v).as_str())?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SubscribeRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tables",
            "prefix",
            "visible",
            "from_snapshot",
            "fromSnapshot",
            "from_oldest",
            "fromOldest",
            "resume",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Tables,
            Prefix,
            Visible,
            FromSnapshot,
            FromOldest,
            Resume,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tables" => Ok(GeneratedField::Tables),
                            "prefix" => Ok(GeneratedField::Prefix),
                            "visible" => Ok(GeneratedField::Visible),
                            "fromSnapshot" | "from_snapshot" => Ok(GeneratedField::FromSnapshot),
                            "fromOldest" | "from_oldest" => Ok(GeneratedField::FromOldest),
                            "resume" => Ok(GeneratedField::Resume),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SubscribeRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.SubscribeRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SubscribeRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut tables__ = None;
                let mut prefix__ = None;
                let mut visible__ = None;
                let mut start__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Tables => {
                            if tables__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tables"));
                            }
                            tables__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Prefix => {
                            if prefix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("prefix"));
                            }
                            prefix__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Visible => {
                            if visible__.is_some() {
                                return Err(serde::de::Error::duplicate_field("visible"));
                            }
                            visible__ = Some(map_.next_value()?);
                        }
                        GeneratedField::FromSnapshot => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fromSnapshot"));
                            }
                            start__ = map_.next_value::<::std::option::Option<_>>()?.map(subscribe_request::Start::FromSnapshot);
                        }
                        GeneratedField::FromOldest => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fromOldest"));
                            }
                            start__ = map_.next_value::<::std::option::Option<_>>()?.map(subscribe_request::Start::FromOldest);
                        }
                        GeneratedField::Resume => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resume"));
                            }
                            start__ = map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| subscribe_request::Start::Resume(x.0));
                        }
                    }
                }
                Ok(SubscribeRequest {
                    tables: tables__.unwrap_or_default(),
                    prefix: prefix__.unwrap_or_default(),
                    visible: visible__.unwrap_or_default(),
                    start: start__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.SubscribeRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SupersedeRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.old.is_empty() {
            len += 1;
        }
        if !self.new.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.SupersedeRequest", len)?;
        if !self.old.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("old", pbjson::private::base64::encode(&self.old).as_str())?;
        }
        if !self.new.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("new", pbjson::private::base64::encode(&self.new).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SupersedeRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "old",
            "new",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Old,
            New,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "old" => Ok(GeneratedField::Old),
                            "new" => Ok(GeneratedField::New),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SupersedeRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.SupersedeRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SupersedeRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut old__ = None;
                let mut new__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Old => {
                            if old__.is_some() {
                                return Err(serde::de::Error::duplicate_field("old"));
                            }
                            old__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::New => {
                            if new__.is_some() {
                                return Err(serde::de::Error::duplicate_field("new"));
                            }
                            new__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(SupersedeRequest {
                    old: old__.unwrap_or_default(),
                    new: new__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.SupersedeRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.families.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TableResponse", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.families.is_empty() {
            struct_ser.serialize_field("families", &self.families)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "families",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Families,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "families" => Ok(GeneratedField::Families),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TableResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TableResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut families__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Families => {
                            if families__.is_some() {
                                return Err(serde::de::Error::duplicate_field("families"));
                            }
                            families__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TableResponse {
                    name: name__.unwrap_or_default(),
                    families: families__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TableResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TextClause {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.clause.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TextClause", len)?;
        if let Some(v) = self.clause.as_ref() {
            match v {
                text_clause::Clause::Term(v) => {
                    struct_ser.serialize_field("term", v)?;
                }
                text_clause::Clause::Phrase(v) => {
                    struct_ser.serialize_field("phrase", v)?;
                }
                text_clause::Clause::Fuzzy(v) => {
                    struct_ser.serialize_field("fuzzy", v)?;
                }
                text_clause::Clause::Text(v) => {
                    struct_ser.serialize_field("text", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TextClause {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "term",
            "phrase",
            "fuzzy",
            "text",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Term,
            Phrase,
            Fuzzy,
            Text,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "term" => Ok(GeneratedField::Term),
                            "phrase" => Ok(GeneratedField::Phrase),
                            "fuzzy" => Ok(GeneratedField::Fuzzy),
                            "text" => Ok(GeneratedField::Text),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TextClause;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TextClause")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TextClause, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut clause__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Term => {
                            if clause__.is_some() {
                                return Err(serde::de::Error::duplicate_field("term"));
                            }
                            clause__ = map_.next_value::<::std::option::Option<_>>()?.map(text_clause::Clause::Term);
                        }
                        GeneratedField::Phrase => {
                            if clause__.is_some() {
                                return Err(serde::de::Error::duplicate_field("phrase"));
                            }
                            clause__ = map_.next_value::<::std::option::Option<_>>()?.map(text_clause::Clause::Phrase)
;
                        }
                        GeneratedField::Fuzzy => {
                            if clause__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fuzzy"));
                            }
                            clause__ = map_.next_value::<::std::option::Option<_>>()?.map(text_clause::Clause::Fuzzy)
;
                        }
                        GeneratedField::Text => {
                            if clause__.is_some() {
                                return Err(serde::de::Error::duplicate_field("text"));
                            }
                            clause__ = map_.next_value::<::std::option::Option<_>>()?.map(text_clause::Clause::Text);
                        }
                    }
                }
                Ok(TextClause {
                    clause: clause__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TextClause", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TextDeleteRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.index.is_empty() {
            len += 1;
        }
        if !self.id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TextDeleteRequest", len)?;
        if !self.index.is_empty() {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if !self.id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id", pbjson::private::base64::encode(&self.id).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TextDeleteRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "id",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            Id,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
                            "id" => Ok(GeneratedField::Id),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TextDeleteRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TextDeleteRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TextDeleteRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(TextDeleteRequest {
                    index: index__.unwrap_or_default(),
                    id: id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TextDeleteRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TextHit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if self.score != 0. {
            len += 1;
        }
        if !self.text.is_empty() {
            len += 1;
        }
        if !self.matches.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TextHit", len)?;
        if !self.id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id", pbjson::private::base64::encode(&self.id).as_str())?;
        }
        if self.score != 0. {
            struct_ser.serialize_field("score", &self.score)?;
        }
        if !self.text.is_empty() {
            struct_ser.serialize_field("text", &self.text)?;
        }
        if !self.matches.is_empty() {
            struct_ser.serialize_field("matches", &self.matches)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TextHit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "score",
            "text",
            "matches",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Score,
            Text,
            Matches,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "score" => Ok(GeneratedField::Score),
                            "text" => Ok(GeneratedField::Text),
                            "matches" => Ok(GeneratedField::Matches),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TextHit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TextHit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TextHit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut score__ = None;
                let mut text__ = None;
                let mut matches__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Score => {
                            if score__.is_some() {
                                return Err(serde::de::Error::duplicate_field("score"));
                            }
                            score__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Text => {
                            if text__.is_some() {
                                return Err(serde::de::Error::duplicate_field("text"));
                            }
                            text__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Matches => {
                            if matches__.is_some() {
                                return Err(serde::de::Error::duplicate_field("matches"));
                            }
                            matches__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TextHit {
                    id: id__.unwrap_or_default(),
                    score: score__.unwrap_or_default(),
                    text: text__.unwrap_or_default(),
                    matches: matches__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TextHit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TextIndexRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.index.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TextIndexRequest", len)?;
        if !self.index.is_empty() {
            struct_ser.serialize_field("index", &self.index)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TextIndexRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TextIndexRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TextIndexRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TextIndexRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TextIndexRequest {
                    index: index__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TextIndexRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TextMatch {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.clause != 0 {
            len += 1;
        }
        if self.occurrence != 0 {
            len += 1;
        }
        if self.start != 0 {
            len += 1;
        }
        if self.end != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TextMatch", len)?;
        if self.clause != 0 {
            struct_ser.serialize_field("clause", &self.clause)?;
        }
        if self.occurrence != 0 {
            struct_ser.serialize_field("occurrence", &self.occurrence)?;
        }
        if self.start != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("start", ToString::to_string(&self.start).as_str())?;
        }
        if self.end != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("end", ToString::to_string(&self.end).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TextMatch {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "clause",
            "occurrence",
            "start",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Clause,
            Occurrence,
            Start,
            End,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "clause" => Ok(GeneratedField::Clause),
                            "occurrence" => Ok(GeneratedField::Occurrence),
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TextMatch;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TextMatch")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TextMatch, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut clause__ = None;
                let mut occurrence__ = None;
                let mut start__ = None;
                let mut end__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Clause => {
                            if clause__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clause"));
                            }
                            clause__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Occurrence => {
                            if occurrence__.is_some() {
                                return Err(serde::de::Error::duplicate_field("occurrence"));
                            }
                            occurrence__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(TextMatch {
                    clause: clause__.unwrap_or_default(),
                    occurrence: occurrence__.unwrap_or_default(),
                    start: start__.unwrap_or_default(),
                    end: end__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TextMatch", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TextPutRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.index.is_empty() {
            len += 1;
        }
        if !self.id.is_empty() {
            len += 1;
        }
        if !self.text.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TextPutRequest", len)?;
        if !self.index.is_empty() {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if !self.id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id", pbjson::private::base64::encode(&self.id).as_str())?;
        }
        if !self.text.is_empty() {
            struct_ser.serialize_field("text", &self.text)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TextPutRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "id",
            "text",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            Id,
            Text,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
                            "id" => Ok(GeneratedField::Id),
                            "text" => Ok(GeneratedField::Text),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TextPutRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TextPutRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TextPutRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut id__ = None;
                let mut text__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Text => {
                            if text__.is_some() {
                                return Err(serde::de::Error::duplicate_field("text"));
                            }
                            text__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TextPutRequest {
                    index: index__.unwrap_or_default(),
                    id: id__.unwrap_or_default(),
                    text: text__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TextPutRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TextQuery {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.clauses.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TextQuery", len)?;
        if !self.clauses.is_empty() {
            struct_ser.serialize_field("clauses", &self.clauses)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TextQuery {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "clauses",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Clauses,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "clauses" => Ok(GeneratedField::Clauses),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TextQuery;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TextQuery")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TextQuery, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut clauses__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Clauses => {
                            if clauses__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clauses"));
                            }
                            clauses__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TextQuery {
                    clauses: clauses__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TextQuery", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TextRebuildResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.documents != 0 {
            len += 1;
        }
        if self.changed != 0 {
            len += 1;
        }
        if self.invalid_utf8 != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TextRebuildResponse", len)?;
        if self.documents != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("documents", ToString::to_string(&self.documents).as_str())?;
        }
        if self.changed != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("changed", ToString::to_string(&self.changed).as_str())?;
        }
        if self.invalid_utf8 != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("invalid_utf8", ToString::to_string(&self.invalid_utf8).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TextRebuildResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "documents",
            "changed",
            "invalid_utf8",
            "invalidUtf8",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Documents,
            Changed,
            InvalidUtf8,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "documents" => Ok(GeneratedField::Documents),
                            "changed" => Ok(GeneratedField::Changed),
                            "invalidUtf8" | "invalid_utf8" => Ok(GeneratedField::InvalidUtf8),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TextRebuildResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TextRebuildResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TextRebuildResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut documents__ = None;
                let mut changed__ = None;
                let mut invalid_utf8__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Documents => {
                            if documents__.is_some() {
                                return Err(serde::de::Error::duplicate_field("documents"));
                            }
                            documents__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Changed => {
                            if changed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("changed"));
                            }
                            changed__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::InvalidUtf8 => {
                            if invalid_utf8__.is_some() {
                                return Err(serde::de::Error::duplicate_field("invalidUtf8"));
                            }
                            invalid_utf8__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(TextRebuildResponse {
                    documents: documents__.unwrap_or_default(),
                    changed: changed__.unwrap_or_default(),
                    invalid_utf8: invalid_utf8__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TextRebuildResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TextSearchRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.index.is_empty() {
            len += 1;
        }
        if self.k != 0 {
            len += 1;
        }
        if !self.id_prefix.is_empty() {
            len += 1;
        }
        if self.highlight {
            len += 1;
        }
        if self.query.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TextSearchRequest", len)?;
        if !self.index.is_empty() {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if self.k != 0 {
            struct_ser.serialize_field("k", &self.k)?;
        }
        if !self.id_prefix.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id_prefix", pbjson::private::base64::encode(&self.id_prefix).as_str())?;
        }
        if self.highlight {
            struct_ser.serialize_field("highlight", &self.highlight)?;
        }
        if let Some(v) = self.query.as_ref() {
            match v {
                text_search_request::Query::Syntax(v) => {
                    struct_ser.serialize_field("syntax", v)?;
                }
                text_search_request::Query::Clauses(v) => {
                    struct_ser.serialize_field("clauses", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TextSearchRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "k",
            "id_prefix",
            "idPrefix",
            "highlight",
            "syntax",
            "clauses",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            K,
            IdPrefix,
            Highlight,
            Syntax,
            Clauses,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
                            "k" => Ok(GeneratedField::K),
                            "idPrefix" | "id_prefix" => Ok(GeneratedField::IdPrefix),
                            "highlight" => Ok(GeneratedField::Highlight),
                            "syntax" => Ok(GeneratedField::Syntax),
                            "clauses" => Ok(GeneratedField::Clauses),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TextSearchRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TextSearchRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TextSearchRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut k__ = None;
                let mut id_prefix__ = None;
                let mut highlight__ = None;
                let mut query__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = Some(map_.next_value()?);
                        }
                        GeneratedField::K => {
                            if k__.is_some() {
                                return Err(serde::de::Error::duplicate_field("k"));
                            }
                            k__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::IdPrefix => {
                            if id_prefix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("idPrefix"));
                            }
                            id_prefix__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Highlight => {
                            if highlight__.is_some() {
                                return Err(serde::de::Error::duplicate_field("highlight"));
                            }
                            highlight__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Syntax => {
                            if query__.is_some() {
                                return Err(serde::de::Error::duplicate_field("syntax"));
                            }
                            query__ = map_.next_value::<::std::option::Option<_>>()?.map(text_search_request::Query::Syntax);
                        }
                        GeneratedField::Clauses => {
                            if query__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clauses"));
                            }
                            query__ = map_.next_value::<::std::option::Option<_>>()?.map(text_search_request::Query::Clauses)
;
                        }
                    }
                }
                Ok(TextSearchRequest {
                    index: index__.unwrap_or_default(),
                    k: k__.unwrap_or_default(),
                    id_prefix: id_prefix__.unwrap_or_default(),
                    highlight: highlight__.unwrap_or_default(),
                    query: query__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TextSearchRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TextSearchResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.hits.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TextSearchResponse", len)?;
        if !self.hits.is_empty() {
            struct_ser.serialize_field("hits", &self.hits)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TextSearchResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hits",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Hits,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "hits" => Ok(GeneratedField::Hits),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TextSearchResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TextSearchResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TextSearchResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hits__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Hits => {
                            if hits__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hits"));
                            }
                            hits__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TextSearchResponse {
                    hits: hits__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TextSearchResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TextVerifyResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.documents != 0 {
            len += 1;
        }
        if self.terms != 0 {
            len += 1;
        }
        if self.postings != 0 {
            len += 1;
        }
        if self.forward_mismatch != 0 {
            len += 1;
        }
        if self.source_mismatch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TextVerifyResponse", len)?;
        if self.documents != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("documents", ToString::to_string(&self.documents).as_str())?;
        }
        if self.terms != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("terms", ToString::to_string(&self.terms).as_str())?;
        }
        if self.postings != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("postings", ToString::to_string(&self.postings).as_str())?;
        }
        if self.forward_mismatch != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("forward_mismatch", ToString::to_string(&self.forward_mismatch).as_str())?;
        }
        if self.source_mismatch != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("source_mismatch", ToString::to_string(&self.source_mismatch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TextVerifyResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "documents",
            "terms",
            "postings",
            "forward_mismatch",
            "forwardMismatch",
            "source_mismatch",
            "sourceMismatch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Documents,
            Terms,
            Postings,
            ForwardMismatch,
            SourceMismatch,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "documents" => Ok(GeneratedField::Documents),
                            "terms" => Ok(GeneratedField::Terms),
                            "postings" => Ok(GeneratedField::Postings),
                            "forwardMismatch" | "forward_mismatch" => Ok(GeneratedField::ForwardMismatch),
                            "sourceMismatch" | "source_mismatch" => Ok(GeneratedField::SourceMismatch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TextVerifyResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TextVerifyResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TextVerifyResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut documents__ = None;
                let mut terms__ = None;
                let mut postings__ = None;
                let mut forward_mismatch__ = None;
                let mut source_mismatch__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Documents => {
                            if documents__.is_some() {
                                return Err(serde::de::Error::duplicate_field("documents"));
                            }
                            documents__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Terms => {
                            if terms__.is_some() {
                                return Err(serde::de::Error::duplicate_field("terms"));
                            }
                            terms__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Postings => {
                            if postings__.is_some() {
                                return Err(serde::de::Error::duplicate_field("postings"));
                            }
                            postings__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ForwardMismatch => {
                            if forward_mismatch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("forwardMismatch"));
                            }
                            forward_mismatch__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SourceMismatch => {
                            if source_mismatch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceMismatch"));
                            }
                            source_mismatch__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(TextVerifyResponse {
                    documents: documents__.unwrap_or_default(),
                    terms: terms__.unwrap_or_default(),
                    postings: postings__.unwrap_or_default(),
                    forward_mismatch: forward_mismatch__.unwrap_or_default(),
                    source_mismatch: source_mismatch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TextVerifyResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TimeRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start != 0 {
            len += 1;
        }
        if self.end != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TimeRange", len)?;
        if self.start != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("start", ToString::to_string(&self.start).as_str())?;
        }
        if self.end != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("end", ToString::to_string(&self.end).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TimeRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TimeRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TimeRange")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TimeRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(TimeRange {
                    start: start__.unwrap_or_default(),
                    end: end__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TimeRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TraverseRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table.is_empty() {
            len += 1;
        }
        if !self.seeds.is_empty() {
            len += 1;
        }
        if !self.family.is_empty() {
            len += 1;
        }
        if self.hops != 0 {
            len += 1;
        }
        if self.per_node_limit != 0 {
            len += 1;
        }
        if self.hop_limit != 0 {
            len += 1;
        }
        if self.max_nodes != 0 {
            len += 1;
        }
        if self.parents {
            len += 1;
        }
        if !self.qualifier_prefix.is_empty() {
            len += 1;
        }
        if self.qualifier_start.is_some() {
            len += 1;
        }
        if self.qualifier_end.is_some() {
            len += 1;
        }
        if self.time_range.is_some() {
            len += 1;
        }
        if self.value_filter.is_some() {
            len += 1;
        }
        if !self.snapshot_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TraverseRequest", len)?;
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        if !self.seeds.is_empty() {
            struct_ser.serialize_field("seeds", &self.seeds.iter().map(pbjson::private::base64::encode).collect::<Vec<_>>())?;
        }
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if self.hops != 0 {
            struct_ser.serialize_field("hops", &self.hops)?;
        }
        if self.per_node_limit != 0 {
            struct_ser.serialize_field("per_node_limit", &self.per_node_limit)?;
        }
        if self.hop_limit != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("hop_limit", ToString::to_string(&self.hop_limit).as_str())?;
        }
        if self.max_nodes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("max_nodes", ToString::to_string(&self.max_nodes).as_str())?;
        }
        if self.parents {
            struct_ser.serialize_field("parents", &self.parents)?;
        }
        if !self.qualifier_prefix.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier_prefix", pbjson::private::base64::encode(&self.qualifier_prefix).as_str())?;
        }
        if let Some(v) = self.qualifier_start.as_ref() {
            struct_ser.serialize_field("qualifier_start", v)?;
        }
        if let Some(v) = self.qualifier_end.as_ref() {
            struct_ser.serialize_field("qualifier_end", v)?;
        }
        if let Some(v) = self.time_range.as_ref() {
            struct_ser.serialize_field("time_range", v)?;
        }
        if let Some(v) = self.value_filter.as_ref() {
            struct_ser.serialize_field("value_filter", v)?;
        }
        if !self.snapshot_id.is_empty() {
            struct_ser.serialize_field("snapshot_id", &self.snapshot_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TraverseRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table",
            "seeds",
            "family",
            "hops",
            "per_node_limit",
            "perNodeLimit",
            "hop_limit",
            "hopLimit",
            "max_nodes",
            "maxNodes",
            "parents",
            "qualifier_prefix",
            "qualifierPrefix",
            "qualifier_start",
            "qualifierStart",
            "qualifier_end",
            "qualifierEnd",
            "time_range",
            "timeRange",
            "value_filter",
            "valueFilter",
            "snapshot_id",
            "snapshotId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Table,
            Seeds,
            Family,
            Hops,
            PerNodeLimit,
            HopLimit,
            MaxNodes,
            Parents,
            QualifierPrefix,
            QualifierStart,
            QualifierEnd,
            TimeRange,
            ValueFilter,
            SnapshotId,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "table" => Ok(GeneratedField::Table),
                            "seeds" => Ok(GeneratedField::Seeds),
                            "family" => Ok(GeneratedField::Family),
                            "hops" => Ok(GeneratedField::Hops),
                            "perNodeLimit" | "per_node_limit" => Ok(GeneratedField::PerNodeLimit),
                            "hopLimit" | "hop_limit" => Ok(GeneratedField::HopLimit),
                            "maxNodes" | "max_nodes" => Ok(GeneratedField::MaxNodes),
                            "parents" => Ok(GeneratedField::Parents),
                            "qualifierPrefix" | "qualifier_prefix" => Ok(GeneratedField::QualifierPrefix),
                            "qualifierStart" | "qualifier_start" => Ok(GeneratedField::QualifierStart),
                            "qualifierEnd" | "qualifier_end" => Ok(GeneratedField::QualifierEnd),
                            "timeRange" | "time_range" => Ok(GeneratedField::TimeRange),
                            "valueFilter" | "value_filter" => Ok(GeneratedField::ValueFilter),
                            "snapshotId" | "snapshot_id" => Ok(GeneratedField::SnapshotId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TraverseRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TraverseRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TraverseRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table__ = None;
                let mut seeds__ = None;
                let mut family__ = None;
                let mut hops__ = None;
                let mut per_node_limit__ = None;
                let mut hop_limit__ = None;
                let mut max_nodes__ = None;
                let mut parents__ = None;
                let mut qualifier_prefix__ = None;
                let mut qualifier_start__ = None;
                let mut qualifier_end__ = None;
                let mut time_range__ = None;
                let mut value_filter__ = None;
                let mut snapshot_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Seeds => {
                            if seeds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("seeds"));
                            }
                            seeds__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Hops => {
                            if hops__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hops"));
                            }
                            hops__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PerNodeLimit => {
                            if per_node_limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("perNodeLimit"));
                            }
                            per_node_limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::HopLimit => {
                            if hop_limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hopLimit"));
                            }
                            hop_limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MaxNodes => {
                            if max_nodes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxNodes"));
                            }
                            max_nodes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Parents => {
                            if parents__.is_some() {
                                return Err(serde::de::Error::duplicate_field("parents"));
                            }
                            parents__ = Some(map_.next_value()?);
                        }
                        GeneratedField::QualifierPrefix => {
                            if qualifier_prefix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifierPrefix"));
                            }
                            qualifier_prefix__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::QualifierStart => {
                            if qualifier_start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifierStart"));
                            }
                            qualifier_start__ = map_.next_value()?;
                        }
                        GeneratedField::QualifierEnd => {
                            if qualifier_end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifierEnd"));
                            }
                            qualifier_end__ = map_.next_value()?;
                        }
                        GeneratedField::TimeRange => {
                            if time_range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeRange"));
                            }
                            time_range__ = map_.next_value()?;
                        }
                        GeneratedField::ValueFilter => {
                            if value_filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("valueFilter"));
                            }
                            value_filter__ = map_.next_value()?;
                        }
                        GeneratedField::SnapshotId => {
                            if snapshot_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotId"));
                            }
                            snapshot_id__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TraverseRequest {
                    table: table__.unwrap_or_default(),
                    seeds: seeds__.unwrap_or_default(),
                    family: family__.unwrap_or_default(),
                    hops: hops__.unwrap_or_default(),
                    per_node_limit: per_node_limit__.unwrap_or_default(),
                    hop_limit: hop_limit__.unwrap_or_default(),
                    max_nodes: max_nodes__.unwrap_or_default(),
                    parents: parents__.unwrap_or_default(),
                    qualifier_prefix: qualifier_prefix__.unwrap_or_default(),
                    qualifier_start: qualifier_start__,
                    qualifier_end: qualifier_end__,
                    time_range: time_range__,
                    value_filter: value_filter__,
                    snapshot_id: snapshot_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TraverseRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TraverseResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.hops.is_empty() {
            len += 1;
        }
        if self.stats.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TraverseResponse", len)?;
        if !self.hops.is_empty() {
            struct_ser.serialize_field("hops", &self.hops)?;
        }
        if let Some(v) = self.stats.as_ref() {
            struct_ser.serialize_field("stats", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TraverseResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hops",
            "stats",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Hops,
            Stats,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "hops" => Ok(GeneratedField::Hops),
                            "stats" => Ok(GeneratedField::Stats),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TraverseResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TraverseResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TraverseResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hops__ = None;
                let mut stats__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Hops => {
                            if hops__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hops"));
                            }
                            hops__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Stats => {
                            if stats__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stats"));
                            }
                            stats__ = map_.next_value()?;
                        }
                    }
                }
                Ok(TraverseResponse {
                    hops: hops__.unwrap_or_default(),
                    stats: stats__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TraverseResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TraverseStats {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.rows != 0 {
            len += 1;
        }
        if self.edges != 0 {
            len += 1;
        }
        if self.hops_cut != 0 {
            len += 1;
        }
        if self.cut_by_max_nodes {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TraverseStats", len)?;
        if self.rows != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("rows", ToString::to_string(&self.rows).as_str())?;
        }
        if self.edges != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("edges", ToString::to_string(&self.edges).as_str())?;
        }
        if self.hops_cut != 0 {
            struct_ser.serialize_field("hops_cut", &self.hops_cut)?;
        }
        if self.cut_by_max_nodes {
            struct_ser.serialize_field("cut_by_max_nodes", &self.cut_by_max_nodes)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TraverseStats {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "rows",
            "edges",
            "hops_cut",
            "hopsCut",
            "cut_by_max_nodes",
            "cutByMaxNodes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Rows,
            Edges,
            HopsCut,
            CutByMaxNodes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "rows" => Ok(GeneratedField::Rows),
                            "edges" => Ok(GeneratedField::Edges),
                            "hopsCut" | "hops_cut" => Ok(GeneratedField::HopsCut),
                            "cutByMaxNodes" | "cut_by_max_nodes" => Ok(GeneratedField::CutByMaxNodes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TraverseStats;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TraverseStats")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TraverseStats, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut rows__ = None;
                let mut edges__ = None;
                let mut hops_cut__ = None;
                let mut cut_by_max_nodes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Rows => {
                            if rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rows"));
                            }
                            rows__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Edges => {
                            if edges__.is_some() {
                                return Err(serde::de::Error::duplicate_field("edges"));
                            }
                            edges__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::HopsCut => {
                            if hops_cut__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hopsCut"));
                            }
                            hops_cut__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::CutByMaxNodes => {
                            if cut_by_max_nodes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cutByMaxNodes"));
                            }
                            cut_by_max_nodes__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TraverseStats {
                    rows: rows__.unwrap_or_default(),
                    edges: edges__.unwrap_or_default(),
                    hops_cut: hops_cut__.unwrap_or_default(),
                    cut_by_max_nodes: cut_by_max_nodes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TraverseStats", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TxnAbortRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.txn_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TxnAbortRequest", len)?;
        if !self.txn_id.is_empty() {
            struct_ser.serialize_field("txn_id", &self.txn_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TxnAbortRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "txn_id",
            "txnId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TxnId,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "txnId" | "txn_id" => Ok(GeneratedField::TxnId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TxnAbortRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TxnAbortRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TxnAbortRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut txn_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TxnId => {
                            if txn_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("txnId"));
                            }
                            txn_id__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TxnAbortRequest {
                    txn_id: txn_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TxnAbortRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TxnCommitRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.txn_id.is_empty() {
            len += 1;
        }
        if !self.rows.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TxnCommitRequest", len)?;
        if !self.txn_id.is_empty() {
            struct_ser.serialize_field("txn_id", &self.txn_id)?;
        }
        if !self.rows.is_empty() {
            struct_ser.serialize_field("rows", &self.rows)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TxnCommitRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "txn_id",
            "txnId",
            "rows",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TxnId,
            Rows,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "txnId" | "txn_id" => Ok(GeneratedField::TxnId),
                            "rows" => Ok(GeneratedField::Rows),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TxnCommitRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TxnCommitRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TxnCommitRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut txn_id__ = None;
                let mut rows__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TxnId => {
                            if txn_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("txnId"));
                            }
                            txn_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Rows => {
                            if rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rows"));
                            }
                            rows__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TxnCommitRequest {
                    txn_id: txn_id__.unwrap_or_default(),
                    rows: rows__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TxnCommitRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TxnCommitResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.commit.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TxnCommitResponse", len)?;
        if let Some(v) = self.commit.as_ref() {
            struct_ser.serialize_field("commit", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TxnCommitResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "commit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Commit,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "commit" => Ok(GeneratedField::Commit),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TxnCommitResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TxnCommitResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TxnCommitResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut commit__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Commit => {
                            if commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("commit"));
                            }
                            commit__ = map_.next_value()?;
                        }
                    }
                }
                Ok(TxnCommitResponse {
                    commit: commit__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TxnCommitResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TxnGetRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.txn_id.is_empty() {
            len += 1;
        }
        if !self.table.is_empty() {
            len += 1;
        }
        if !self.row.is_empty() {
            len += 1;
        }
        if !self.family.is_empty() {
            len += 1;
        }
        if !self.qualifier.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TxnGetRequest", len)?;
        if !self.txn_id.is_empty() {
            struct_ser.serialize_field("txn_id", &self.txn_id)?;
        }
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        if !self.row.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("row", pbjson::private::base64::encode(&self.row).as_str())?;
        }
        if !self.family.is_empty() {
            struct_ser.serialize_field("family", &self.family)?;
        }
        if !self.qualifier.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("qualifier", pbjson::private::base64::encode(&self.qualifier).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TxnGetRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "txn_id",
            "txnId",
            "table",
            "row",
            "family",
            "qualifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TxnId,
            Table,
            Row,
            Family,
            Qualifier,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "txnId" | "txn_id" => Ok(GeneratedField::TxnId),
                            "table" => Ok(GeneratedField::Table),
                            "row" => Ok(GeneratedField::Row),
                            "family" => Ok(GeneratedField::Family),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TxnGetRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TxnGetRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TxnGetRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut txn_id__ = None;
                let mut table__ = None;
                let mut row__ = None;
                let mut family__ = None;
                let mut qualifier__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TxnId => {
                            if txn_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("txnId"));
                            }
                            txn_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Row => {
                            if row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("row"));
                            }
                            row__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Family => {
                            if family__.is_some() {
                                return Err(serde::de::Error::duplicate_field("family"));
                            }
                            family__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(TxnGetRequest {
                    txn_id: txn_id__.unwrap_or_default(),
                    table: table__.unwrap_or_default(),
                    row: row__.unwrap_or_default(),
                    family: family__.unwrap_or_default(),
                    qualifier: qualifier__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TxnGetRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TxnGetRowRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.txn_id.is_empty() {
            len += 1;
        }
        if !self.table.is_empty() {
            len += 1;
        }
        if !self.row.is_empty() {
            len += 1;
        }
        if !self.families.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TxnGetRowRequest", len)?;
        if !self.txn_id.is_empty() {
            struct_ser.serialize_field("txn_id", &self.txn_id)?;
        }
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        if !self.row.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("row", pbjson::private::base64::encode(&self.row).as_str())?;
        }
        if !self.families.is_empty() {
            struct_ser.serialize_field("families", &self.families)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TxnGetRowRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "txn_id",
            "txnId",
            "table",
            "row",
            "families",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TxnId,
            Table,
            Row,
            Families,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "txnId" | "txn_id" => Ok(GeneratedField::TxnId),
                            "table" => Ok(GeneratedField::Table),
                            "row" => Ok(GeneratedField::Row),
                            "families" => Ok(GeneratedField::Families),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TxnGetRowRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TxnGetRowRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TxnGetRowRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut txn_id__ = None;
                let mut table__ = None;
                let mut row__ = None;
                let mut families__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TxnId => {
                            if txn_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("txnId"));
                            }
                            txn_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Row => {
                            if row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("row"));
                            }
                            row__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Families => {
                            if families__.is_some() {
                                return Err(serde::de::Error::duplicate_field("families"));
                            }
                            families__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TxnGetRowRequest {
                    txn_id: txn_id__.unwrap_or_default(),
                    table: table__.unwrap_or_default(),
                    row: row__.unwrap_or_default(),
                    families: families__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TxnGetRowRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TxnWriteRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.txn_id.is_empty() {
            len += 1;
        }
        if !self.rows.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.TxnWriteRequest", len)?;
        if !self.txn_id.is_empty() {
            struct_ser.serialize_field("txn_id", &self.txn_id)?;
        }
        if !self.rows.is_empty() {
            struct_ser.serialize_field("rows", &self.rows)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TxnWriteRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "txn_id",
            "txnId",
            "rows",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TxnId,
            Rows,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "txnId" | "txn_id" => Ok(GeneratedField::TxnId),
                            "rows" => Ok(GeneratedField::Rows),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TxnWriteRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.TxnWriteRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TxnWriteRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut txn_id__ = None;
                let mut rows__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TxnId => {
                            if txn_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("txnId"));
                            }
                            txn_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Rows => {
                            if rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rows"));
                            }
                            rows__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(TxnWriteRequest {
                    txn_id: txn_id__.unwrap_or_default(),
                    rows: rows__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.TxnWriteRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Value {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.kind.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Value", len)?;
        if let Some(v) = self.kind.as_ref() {
            match v {
                value::Kind::Bytes(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("bytes", pbjson::private::base64::encode(&v).as_str())?;
                }
                value::Kind::I64(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("i64", ToString::to_string(&v).as_str())?;
                }
                value::Kind::F64(v) => {
                    struct_ser.serialize_field("f64", v)?;
                }
                value::Kind::U64(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("u64", ToString::to_string(&v).as_str())?;
                }
                value::Kind::Vector(v) => {
                    struct_ser.serialize_field("vector", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Value {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "bytes",
            "i64",
            "f64",
            "u64",
            "vector",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Bytes,
            I64,
            F64,
            U64,
            Vector,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "bytes" => Ok(GeneratedField::Bytes),
                            "i64" => Ok(GeneratedField::I64),
                            "f64" => Ok(GeneratedField::F64),
                            "u64" => Ok(GeneratedField::U64),
                            "vector" => Ok(GeneratedField::Vector),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Value")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Value, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut kind__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Bytes => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bytes"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| value::Kind::Bytes(x.0));
                        }
                        GeneratedField::I64 => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("i64"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| value::Kind::I64(x.0));
                        }
                        GeneratedField::F64 => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("f64"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| value::Kind::F64(x.0));
                        }
                        GeneratedField::U64 => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("u64"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| value::Kind::U64(x.0));
                        }
                        GeneratedField::Vector => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("vector"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<_>>()?.map(value::Kind::Vector)
;
                        }
                    }
                }
                Ok(Value {
                    kind: kind__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Value", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ValueCondition {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.column.is_some() {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ValueCondition", len)?;
        if let Some(v) = self.column.as_ref() {
            struct_ser.serialize_field("column", v)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ValueCondition {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "column",
            "filter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Column,
            Filter,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "column" => Ok(GeneratedField::Column),
                            "filter" => Ok(GeneratedField::Filter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ValueCondition;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ValueCondition")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ValueCondition, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column__ = None;
                let mut filter__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Column => {
                            if column__.is_some() {
                                return Err(serde::de::Error::duplicate_field("column"));
                            }
                            column__ = map_.next_value()?;
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ValueCondition {
                    column: column__,
                    filter: filter__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ValueCondition", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ValueFilter {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.kind.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.ValueFilter", len)?;
        if let Some(v) = self.kind.as_ref() {
            match v {
                value_filter::Kind::Equals(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("equals", pbjson::private::base64::encode(&v).as_str())?;
                }
                value_filter::Kind::Prefix(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("prefix", pbjson::private::base64::encode(&v).as_str())?;
                }
                value_filter::Kind::I64(v) => {
                    struct_ser.serialize_field("i64", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ValueFilter {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "equals",
            "prefix",
            "i64",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Equals,
            Prefix,
            I64,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "equals" => Ok(GeneratedField::Equals),
                            "prefix" => Ok(GeneratedField::Prefix),
                            "i64" => Ok(GeneratedField::I64),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ValueFilter;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.ValueFilter")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ValueFilter, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut kind__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Equals => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("equals"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| value_filter::Kind::Equals(x.0));
                        }
                        GeneratedField::Prefix => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("prefix"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| value_filter::Kind::Prefix(x.0));
                        }
                        GeneratedField::I64 => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("i64"));
                            }
                            kind__ = map_.next_value::<::std::option::Option<_>>()?.map(value_filter::Kind::I64)
;
                        }
                    }
                }
                Ok(ValueFilter {
                    kind: kind__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.ValueFilter", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Vector {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.element != 0 {
            len += 1;
        }
        if self.dim != 0 {
            len += 1;
        }
        if !self.data.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Vector", len)?;
        if self.element != 0 {
            let v = Element::try_from(self.element)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.element)))?;
            struct_ser.serialize_field("element", &v)?;
        }
        if self.dim != 0 {
            struct_ser.serialize_field("dim", &self.dim)?;
        }
        if !self.data.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("data", pbjson::private::base64::encode(&self.data).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Vector {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "element",
            "dim",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Element,
            Dim,
            Data,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "element" => Ok(GeneratedField::Element),
                            "dim" => Ok(GeneratedField::Dim),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Vector;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Vector")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Vector, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut element__ = None;
                let mut dim__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Element => {
                            if element__.is_some() {
                                return Err(serde::de::Error::duplicate_field("element"));
                            }
                            element__ = Some(map_.next_value::<Element>()? as i32);
                        }
                        GeneratedField::Dim => {
                            if dim__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dim"));
                            }
                            dim__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Vector {
                    element: element__.unwrap_or_default(),
                    dim: dim__.unwrap_or_default(),
                    data: data__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Vector", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VectorBulkLoadRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.index.is_empty() {
            len += 1;
        }
        if self.expected != 0 {
            len += 1;
        }
        if !self.items.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.VectorBulkLoadRequest", len)?;
        if !self.index.is_empty() {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if self.expected != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("expected", ToString::to_string(&self.expected).as_str())?;
        }
        if !self.items.is_empty() {
            struct_ser.serialize_field("items", &self.items)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VectorBulkLoadRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "expected",
            "items",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            Expected,
            Items,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
                            "expected" => Ok(GeneratedField::Expected),
                            "items" => Ok(GeneratedField::Items),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VectorBulkLoadRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.VectorBulkLoadRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VectorBulkLoadRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut expected__ = None;
                let mut items__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Expected => {
                            if expected__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expected"));
                            }
                            expected__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Items => {
                            if items__.is_some() {
                                return Err(serde::de::Error::duplicate_field("items"));
                            }
                            items__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(VectorBulkLoadRequest {
                    index: index__.unwrap_or_default(),
                    expected: expected__.unwrap_or_default(),
                    items: items__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.VectorBulkLoadRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VectorDeleteRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.index.is_empty() {
            len += 1;
        }
        if !self.id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.VectorDeleteRequest", len)?;
        if !self.index.is_empty() {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if !self.id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id", pbjson::private::base64::encode(&self.id).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VectorDeleteRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "id",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            Id,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
                            "id" => Ok(GeneratedField::Id),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VectorDeleteRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.VectorDeleteRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VectorDeleteRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(VectorDeleteRequest {
                    index: index__.unwrap_or_default(),
                    id: id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.VectorDeleteRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VectorHit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if self.distance != 0. {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.VectorHit", len)?;
        if !self.id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id", pbjson::private::base64::encode(&self.id).as_str())?;
        }
        if self.distance != 0. {
            struct_ser.serialize_field("distance", &self.distance)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VectorHit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "distance",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Distance,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "distance" => Ok(GeneratedField::Distance),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VectorHit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.VectorHit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VectorHit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut distance__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Distance => {
                            if distance__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distance"));
                            }
                            distance__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(VectorHit {
                    id: id__.unwrap_or_default(),
                    distance: distance__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.VectorHit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VectorInsertRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.index.is_empty() {
            len += 1;
        }
        if !self.id.is_empty() {
            len += 1;
        }
        if self.vector.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.VectorInsertRequest", len)?;
        if !self.index.is_empty() {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if !self.id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id", pbjson::private::base64::encode(&self.id).as_str())?;
        }
        if let Some(v) = self.vector.as_ref() {
            struct_ser.serialize_field("vector", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VectorInsertRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "id",
            "vector",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            Id,
            Vector,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
                            "id" => Ok(GeneratedField::Id),
                            "vector" => Ok(GeneratedField::Vector),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VectorInsertRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.VectorInsertRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VectorInsertRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut id__ = None;
                let mut vector__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Vector => {
                            if vector__.is_some() {
                                return Err(serde::de::Error::duplicate_field("vector"));
                            }
                            vector__ = map_.next_value()?;
                        }
                    }
                }
                Ok(VectorInsertRequest {
                    index: index__.unwrap_or_default(),
                    id: id__.unwrap_or_default(),
                    vector: vector__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.VectorInsertRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VectorItem {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if self.vector.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.VectorItem", len)?;
        if !self.id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id", pbjson::private::base64::encode(&self.id).as_str())?;
        }
        if let Some(v) = self.vector.as_ref() {
            struct_ser.serialize_field("vector", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VectorItem {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "vector",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Vector,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "vector" => Ok(GeneratedField::Vector),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VectorItem;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.VectorItem")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VectorItem, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut vector__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Vector => {
                            if vector__.is_some() {
                                return Err(serde::de::Error::duplicate_field("vector"));
                            }
                            vector__ = map_.next_value()?;
                        }
                    }
                }
                Ok(VectorItem {
                    id: id__.unwrap_or_default(),
                    vector: vector__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.VectorItem", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VectorSearchRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.index.is_empty() {
            len += 1;
        }
        if self.query.is_some() {
            len += 1;
        }
        if self.k != 0 {
            len += 1;
        }
        if self.nprobe != 0 {
            len += 1;
        }
        if !self.id_prefix.is_empty() {
            len += 1;
        }
        if self.max_probe != 0 {
            len += 1;
        }
        if self.stats {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.VectorSearchRequest", len)?;
        if !self.index.is_empty() {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if let Some(v) = self.query.as_ref() {
            struct_ser.serialize_field("query", v)?;
        }
        if self.k != 0 {
            struct_ser.serialize_field("k", &self.k)?;
        }
        if self.nprobe != 0 {
            struct_ser.serialize_field("nprobe", &self.nprobe)?;
        }
        if !self.id_prefix.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id_prefix", pbjson::private::base64::encode(&self.id_prefix).as_str())?;
        }
        if self.max_probe != 0 {
            struct_ser.serialize_field("max_probe", &self.max_probe)?;
        }
        if self.stats {
            struct_ser.serialize_field("stats", &self.stats)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VectorSearchRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "query",
            "k",
            "nprobe",
            "id_prefix",
            "idPrefix",
            "max_probe",
            "maxProbe",
            "stats",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            Query,
            K,
            Nprobe,
            IdPrefix,
            MaxProbe,
            Stats,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
                            "query" => Ok(GeneratedField::Query),
                            "k" => Ok(GeneratedField::K),
                            "nprobe" => Ok(GeneratedField::Nprobe),
                            "idPrefix" | "id_prefix" => Ok(GeneratedField::IdPrefix),
                            "maxProbe" | "max_probe" => Ok(GeneratedField::MaxProbe),
                            "stats" => Ok(GeneratedField::Stats),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VectorSearchRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.VectorSearchRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VectorSearchRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut query__ = None;
                let mut k__ = None;
                let mut nprobe__ = None;
                let mut id_prefix__ = None;
                let mut max_probe__ = None;
                let mut stats__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Query => {
                            if query__.is_some() {
                                return Err(serde::de::Error::duplicate_field("query"));
                            }
                            query__ = map_.next_value()?;
                        }
                        GeneratedField::K => {
                            if k__.is_some() {
                                return Err(serde::de::Error::duplicate_field("k"));
                            }
                            k__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Nprobe => {
                            if nprobe__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nprobe"));
                            }
                            nprobe__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::IdPrefix => {
                            if id_prefix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("idPrefix"));
                            }
                            id_prefix__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MaxProbe => {
                            if max_probe__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxProbe"));
                            }
                            max_probe__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Stats => {
                            if stats__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stats"));
                            }
                            stats__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(VectorSearchRequest {
                    index: index__.unwrap_or_default(),
                    query: query__,
                    k: k__.unwrap_or_default(),
                    nprobe: nprobe__.unwrap_or_default(),
                    id_prefix: id_prefix__.unwrap_or_default(),
                    max_probe: max_probe__.unwrap_or_default(),
                    stats: stats__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.VectorSearchRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VectorSearchResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.hits.is_empty() {
            len += 1;
        }
        if self.stats.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.VectorSearchResponse", len)?;
        if !self.hits.is_empty() {
            struct_ser.serialize_field("hits", &self.hits)?;
        }
        if let Some(v) = self.stats.as_ref() {
            struct_ser.serialize_field("stats", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VectorSearchResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hits",
            "stats",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Hits,
            Stats,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "hits" => Ok(GeneratedField::Hits),
                            "stats" => Ok(GeneratedField::Stats),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VectorSearchResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.VectorSearchResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VectorSearchResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hits__ = None;
                let mut stats__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Hits => {
                            if hits__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hits"));
                            }
                            hits__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Stats => {
                            if stats__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stats"));
                            }
                            stats__ = map_.next_value()?;
                        }
                    }
                }
                Ok(VectorSearchResponse {
                    hits: hits__.unwrap_or_default(),
                    stats: stats__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.VectorSearchResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VectorSearchStats {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.lists_probed != 0 {
            len += 1;
        }
        if self.lists != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.VectorSearchStats", len)?;
        if self.lists_probed != 0 {
            struct_ser.serialize_field("lists_probed", &self.lists_probed)?;
        }
        if self.lists != 0 {
            struct_ser.serialize_field("lists", &self.lists)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VectorSearchStats {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "lists_probed",
            "listsProbed",
            "lists",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ListsProbed,
            Lists,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "listsProbed" | "lists_probed" => Ok(GeneratedField::ListsProbed),
                            "lists" => Ok(GeneratedField::Lists),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VectorSearchStats;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.VectorSearchStats")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VectorSearchStats, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut lists_probed__ = None;
                let mut lists__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ListsProbed => {
                            if lists_probed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("listsProbed"));
                            }
                            lists_probed__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Lists => {
                            if lists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lists"));
                            }
                            lists__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(VectorSearchStats {
                    lists_probed: lists_probed__.unwrap_or_default(),
                    lists: lists__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.VectorSearchStats", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VectorSpec {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.dim != 0 {
            len += 1;
        }
        if self.element != 0 {
            len += 1;
        }
        if self.metric != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.VectorSpec", len)?;
        if self.dim != 0 {
            struct_ser.serialize_field("dim", &self.dim)?;
        }
        if self.element != 0 {
            let v = Element::try_from(self.element)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.element)))?;
            struct_ser.serialize_field("element", &v)?;
        }
        if self.metric != 0 {
            let v = Metric::try_from(self.metric)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.metric)))?;
            struct_ser.serialize_field("metric", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VectorSpec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "dim",
            "element",
            "metric",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Dim,
            Element,
            Metric,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "dim" => Ok(GeneratedField::Dim),
                            "element" => Ok(GeneratedField::Element),
                            "metric" => Ok(GeneratedField::Metric),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VectorSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.VectorSpec")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VectorSpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut dim__ = None;
                let mut element__ = None;
                let mut metric__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Dim => {
                            if dim__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dim"));
                            }
                            dim__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Element => {
                            if element__.is_some() {
                                return Err(serde::de::Error::duplicate_field("element"));
                            }
                            element__ = Some(map_.next_value::<Element>()? as i32);
                        }
                        GeneratedField::Metric => {
                            if metric__.is_some() {
                                return Err(serde::de::Error::duplicate_field("metric"));
                            }
                            metric__ = Some(map_.next_value::<Metric>()? as i32);
                        }
                    }
                }
                Ok(VectorSpec {
                    dim: dim__.unwrap_or_default(),
                    element: element__.unwrap_or_default(),
                    metric: metric__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.VectorSpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VectorVerifyRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.index.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.VectorVerifyRequest", len)?;
        if !self.index.is_empty() {
            struct_ser.serialize_field("index", &self.index)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VectorVerifyRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VectorVerifyRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.VectorVerifyRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VectorVerifyRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(VectorVerifyRequest {
                    index: index__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.VectorVerifyRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VectorVerifyResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.vectors != 0 {
            len += 1;
        }
        if self.lists != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.VectorVerifyResponse", len)?;
        if self.vectors != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("vectors", ToString::to_string(&self.vectors).as_str())?;
        }
        if self.lists != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("lists", ToString::to_string(&self.lists).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VectorVerifyResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "vectors",
            "lists",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Vectors,
            Lists,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "vectors" => Ok(GeneratedField::Vectors),
                            "lists" => Ok(GeneratedField::Lists),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VectorVerifyResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.VectorVerifyResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VectorVerifyResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut vectors__ = None;
                let mut lists__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Vectors => {
                            if vectors__.is_some() {
                                return Err(serde::de::Error::duplicate_field("vectors"));
                            }
                            vectors__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Lists => {
                            if lists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lists"));
                            }
                            lists__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(VectorVerifyResponse {
                    vectors: vectors__.unwrap_or_default(),
                    lists: lists__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.VectorVerifyResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WatchedMemory {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.namespace.is_empty() {
            len += 1;
        }
        if !self.id.is_empty() {
            len += 1;
        }
        if !self.text.is_empty() {
            len += 1;
        }
        if self.kind != 0 {
            len += 1;
        }
        if self.created != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.WatchedMemory", len)?;
        if !self.namespace.is_empty() {
            struct_ser.serialize_field("namespace", &self.namespace)?;
        }
        if !self.id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id", pbjson::private::base64::encode(&self.id).as_str())?;
        }
        if !self.text.is_empty() {
            struct_ser.serialize_field("text", &self.text)?;
        }
        if self.kind != 0 {
            let v = MemoryKind::try_from(self.kind)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.kind)))?;
            struct_ser.serialize_field("kind", &v)?;
        }
        if self.created != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("created", ToString::to_string(&self.created).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WatchedMemory {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "namespace",
            "id",
            "text",
            "kind",
            "created",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Namespace,
            Id,
            Text,
            Kind,
            Created,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "namespace" => Ok(GeneratedField::Namespace),
                            "id" => Ok(GeneratedField::Id),
                            "text" => Ok(GeneratedField::Text),
                            "kind" => Ok(GeneratedField::Kind),
                            "created" => Ok(GeneratedField::Created),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WatchedMemory;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.WatchedMemory")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WatchedMemory, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut namespace__ = None;
                let mut id__ = None;
                let mut text__ = None;
                let mut kind__ = None;
                let mut created__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Namespace => {
                            if namespace__.is_some() {
                                return Err(serde::de::Error::duplicate_field("namespace"));
                            }
                            namespace__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Text => {
                            if text__.is_some() {
                                return Err(serde::de::Error::duplicate_field("text"));
                            }
                            text__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Kind => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("kind"));
                            }
                            kind__ = Some(map_.next_value::<MemoryKind>()? as i32);
                        }
                        GeneratedField::Created => {
                            if created__.is_some() {
                                return Err(serde::de::Error::duplicate_field("created"));
                            }
                            created__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(WatchedMemory {
                    namespace: namespace__.unwrap_or_default(),
                    id: id__.unwrap_or_default(),
                    text: text__.unwrap_or_default(),
                    kind: kind__.unwrap_or_default(),
                    created: created__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.WatchedMemory", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Write {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.op.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("pigeonhole.v1.Write", len)?;
        if let Some(v) = self.op.as_ref() {
            match v {
                write::Op::Put(v) => {
                    struct_ser.serialize_field("put", v)?;
                }
                write::Op::Incr(v) => {
                    struct_ser.serialize_field("incr", v)?;
                }
                write::Op::Aggregate(v) => {
                    struct_ser.serialize_field("aggregate", v)?;
                }
                write::Op::Merge(v) => {
                    struct_ser.serialize_field("merge", v)?;
                }
                write::Op::DeleteCell(v) => {
                    struct_ser.serialize_field("delete_cell", v)?;
                }
                write::Op::DeleteColumn(v) => {
                    struct_ser.serialize_field("delete_column", v)?;
                }
                write::Op::DeleteFamily(v) => {
                    struct_ser.serialize_field("delete_family", v)?;
                }
                write::Op::DeleteRow(v) => {
                    struct_ser.serialize_field("delete_row", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Write {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "put",
            "incr",
            "aggregate",
            "merge",
            "delete_cell",
            "deleteCell",
            "delete_column",
            "deleteColumn",
            "delete_family",
            "deleteFamily",
            "delete_row",
            "deleteRow",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Put,
            Incr,
            Aggregate,
            Merge,
            DeleteCell,
            DeleteColumn,
            DeleteFamily,
            DeleteRow,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "put" => Ok(GeneratedField::Put),
                            "incr" => Ok(GeneratedField::Incr),
                            "aggregate" => Ok(GeneratedField::Aggregate),
                            "merge" => Ok(GeneratedField::Merge),
                            "deleteCell" | "delete_cell" => Ok(GeneratedField::DeleteCell),
                            "deleteColumn" | "delete_column" => Ok(GeneratedField::DeleteColumn),
                            "deleteFamily" | "delete_family" => Ok(GeneratedField::DeleteFamily),
                            "deleteRow" | "delete_row" => Ok(GeneratedField::DeleteRow),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Write;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct pigeonhole.v1.Write")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Write, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut op__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Put => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("put"));
                            }
                            op__ = map_.next_value::<::std::option::Option<_>>()?.map(write::Op::Put)
;
                        }
                        GeneratedField::Incr => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("incr"));
                            }
                            op__ = map_.next_value::<::std::option::Option<_>>()?.map(write::Op::Incr)
;
                        }
                        GeneratedField::Aggregate => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregate"));
                            }
                            op__ = map_.next_value::<::std::option::Option<_>>()?.map(write::Op::Aggregate)
;
                        }
                        GeneratedField::Merge => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("merge"));
                            }
                            op__ = map_.next_value::<::std::option::Option<_>>()?.map(write::Op::Merge)
;
                        }
                        GeneratedField::DeleteCell => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deleteCell"));
                            }
                            op__ = map_.next_value::<::std::option::Option<_>>()?.map(write::Op::DeleteCell)
;
                        }
                        GeneratedField::DeleteColumn => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deleteColumn"));
                            }
                            op__ = map_.next_value::<::std::option::Option<_>>()?.map(write::Op::DeleteColumn)
;
                        }
                        GeneratedField::DeleteFamily => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deleteFamily"));
                            }
                            op__ = map_.next_value::<::std::option::Option<_>>()?.map(write::Op::DeleteFamily)
;
                        }
                        GeneratedField::DeleteRow => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deleteRow"));
                            }
                            op__ = map_.next_value::<::std::option::Option<_>>()?.map(write::Op::DeleteRow)
;
                        }
                    }
                }
                Ok(Write {
                    op: op__,
                })
            }
        }
        deserializer.deserialize_struct("pigeonhole.v1.Write", FIELDS, GeneratedVisitor)
    }
}
