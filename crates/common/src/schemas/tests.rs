use std::{
    hint::black_box,
    time::Instant,
};

use value::{
    obj,
    val,
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    InternalId,
    TableMapping,
    TableNamespace,
    TableNumber,
    TabletId,
};

use super::{
    validator::{
        FieldValidator,
        FunctionValidator,
        LiteralValidator,
        ObjectValidator,
        ValidationContext,
        ValidationError,
        Validator,
    },
    DocumentSchema,
};
use crate::{
    object_validator,
    virtual_system_mapping::VirtualSystemMapping,
};

#[test]
fn union_alternatives_preserve_acceptance() -> anyhow::Result<()> {
    let table_mapping = TableMapping::new().namespace(TableNamespace::root_component());
    let virtual_mapping = VirtualSystemMapping::default();
    let object = object_validator!(
        "required" => FieldValidator::required_field_type(Validator::Int64),
        "optional" => FieldValidator::optional_field_type(Validator::String),
    );
    let validators = vec![
        Validator::Null,
        Validator::Float64,
        Validator::Int64,
        Validator::CommitTs,
        Validator::Boolean,
        Validator::String,
        Validator::Bytes,
        Validator::Id("items".parse()?),
        Validator::Function(FunctionValidator::default()),
        Validator::Literal(LiteralValidator::Float64(f64::NAN.into())),
        Validator::Literal(LiteralValidator::Float64((-0.0).into())),
        Validator::Literal(LiteralValidator::Int64(1)),
        Validator::Literal(LiteralValidator::Boolean(true)),
        Validator::Literal(LiteralValidator::String("tag".try_into()?)),
        Validator::Array(Box::new(Validator::Int64)),
        Validator::Record(Box::new(Validator::String), Box::new(Validator::Boolean)),
        Validator::Record(
            Box::new(Validator::Literal(LiteralValidator::String(
                "key".try_into()?,
            ))),
            Box::new(Validator::Array(Box::new(Validator::Int64))),
        ),
        Validator::Object(object.clone()),
        Validator::Array(Box::new(Validator::Object(object))),
        Validator::Union(vec![]),
        Validator::Union(vec![Validator::String]),
        Validator::Union(vec![Validator::Int64, Validator::String]),
        Validator::Any,
    ];
    let values = vec![
        val!(null),
        val!(true),
        val!(false),
        val!(1i64),
        val!(2i64),
        val!(1.0),
        val!(0.0),
        val!(-0.0),
        ConvexValue::Float64(f64::NAN),
        val!("tag"),
        val!("other"),
        val!("invalid id"),
        ConvexValue::from(DeveloperDocumentId::MIN),
        val!(format!("function://{}", DeveloperDocumentId::MIN)),
        ConvexValue::Bytes(vec![1, 2].try_into()?),
        val!([]),
        val!([1i64, 2i64]),
        val!([1i64, "wrong"]),
        val!({}),
        val!({"key" => true}),
        val!({"key" => [1i64]}),
        val!({"wrong" => [1i64]}),
        val!({"required" => 1i64}),
        val!({"required" => 1i64, "optional" => "present"}),
        val!({"required" => 1i64, "optional" => false}),
        val!({"required" => 1i64, "extra" => false}),
        val!([{"required" => 1i64}]),
        val!([{"required" => "wrong"}]),
    ];
    for validator in validators {
        // A second impossible alternative exercises validation without diagnostics.
        let union = Validator::Union(vec![validator.clone(), Validator::Union(vec![])]);
        for value in &values {
            assert_eq!(
                union
                    .check_value(value, &table_mapping, &virtual_mapping)
                    .is_ok(),
                validator
                    .check_value(value, &table_mapping, &virtual_mapping)
                    .is_ok(),
                "validator {validator}, value {value}",
            );
        }
    }
    Ok(())
}

#[test]
fn document_unions_preserve_diagnostics_and_system_fields() -> anyhow::Result<()> {
    let table_mapping = TableMapping::new().namespace(TableNamespace::root_component());
    let virtual_mapping = VirtualSystemMapping::default();
    let first = object_validator!(
        "kind" => FieldValidator::required_field_type(Validator::Literal(
            LiteralValidator::String("first".try_into()?),
        )),
        "count" => FieldValidator::optional_field_type(Validator::Int64),
    );
    let second = object_validator!(
        "kind" => FieldValidator::required_field_type(Validator::Literal(
            LiteralValidator::String("second".try_into()?),
        )),
    );
    let documents = vec![
        obj!()?,
        obj!("kind" => "first")?,
        obj!("kind" => "second")?,
        obj!("kind" => "third")?,
        obj!("kind" => "first", "count" => "wrong")?,
        obj!("kind" => "first", "extra" => true)?,
        obj!("kind" => "first", "_id" => "system field", "_creationTime" => 1.0)?,
    ];
    for alternatives in [vec![], vec![first.clone()], vec![first.clone(), second]] {
        let schema = DocumentSchema::Union(alternatives.clone());
        let owned_union =
            Validator::Union(alternatives.into_iter().map(Validator::Object).collect());
        for document in &documents {
            assert_eq!(
                schema.check_value(document, &table_mapping, &virtual_mapping),
                owned_union.check_value(
                    &ConvexValue::Object(document.clone().filter_system_fields()),
                    &table_mapping,
                    &virtual_mapping,
                ),
            );
            assert!(DocumentSchema::Any
                .check_value(document, &table_mapping, &virtual_mapping)
                .is_ok());
        }
    }
    let schema = DocumentSchema::Union(vec![first.clone()]);
    assert_eq!(
        schema.check_value(&obj!()?, &table_mapping, &virtual_mapping),
        Err(ValidationError::MissingRequiredField {
            object: obj!()?,
            field_name: "kind".parse()?,
            object_validator: first.clone(),
            context: ValidationContext::new(),
        }),
    );
    let extra = obj!("kind" => "first", "extra" => true)?;
    assert_eq!(
        schema.check_value(&extra, &table_mapping, &virtual_mapping),
        Err(ValidationError::ExtraField {
            object: extra,
            field_name: "extra".parse()?,
            object_validator: first,
            context: ValidationContext::new(),
        }),
    );
    Ok(())
}

#[test]
fn system_fields_are_filtered_only_at_document_roots() -> anyhow::Result<()> {
    let table_mapping = TableMapping::new().namespace(TableNamespace::root_component());
    let virtual_mapping = VirtualSystemMapping::default();
    let nested_validator = object_validator!();
    let root_validator = object_validator!(
        "nested" => FieldValidator::required_field_type(Validator::Object(nested_validator.clone())),
    );
    let schema = DocumentSchema::Union(vec![root_validator.clone()]);
    let document = obj!("_id" => "root", "nested" => {})?;
    assert_eq!(
        schema.check_value(&document, &table_mapping, &virtual_mapping),
        Ok(()),
    );
    assert_eq!(
        Validator::Object(root_validator.clone()).check_value(
            &ConvexValue::Object(document.clone()),
            &table_mapping,
            &virtual_mapping,
        ),
        Err(ValidationError::ExtraField {
            object: document,
            field_name: "_id".parse()?,
            object_validator: root_validator,
            context: ValidationContext::new(),
        }),
    );

    let nested = obj!("_id" => "nested")?;
    let document = obj!("_id" => "root", "nested" => nested.clone())?;
    let error = schema
        .check_value(&document, &table_mapping, &virtual_mapping)
        .unwrap_err();
    let ValidationError::ExtraField {
        object,
        field_name,
        object_validator,
        context,
    } = &error
    else {
        panic!("Unexpected diagnostic: {error}");
    };
    assert_eq!(object, &nested);
    assert_eq!(field_name.to_string(), "_id");
    assert_eq!(object_validator, &nested_validator);
    assert_eq!(context.to_string(), "Path: .nested");
    assert_eq!(
        error.to_string(),
        "Object contains extra field `_id` that is not in the validator.\nPath: .nested\nObject: \
         {_id: \"nested\"}\nValidator: v.object({})",
    );
    Ok(())
}

#[test]
fn nested_union_diagnostics_preserve_context() -> anyhow::Result<()> {
    let table_mapping = TableMapping::new().namespace(TableNamespace::root_component());
    let virtual_mapping = VirtualSystemMapping::default();
    for alternatives in [
        vec![],
        vec![Validator::Int64],
        vec![Validator::Int64, Validator::Null],
    ] {
        let union = Validator::Union(alternatives.clone());
        let validator = Validator::Object(object_validator!(
            "items" => FieldValidator::required_field_type(Validator::Array(Box::new(
                Validator::Record(Box::new(Validator::String), Box::new(union.clone())),
            ))),
        ));
        let error = validator
            .check_value(
                &val!({"items" => [{"key" => "wrong"}]}),
                &table_mapping,
                &virtual_mapping,
            )
            .unwrap_err();
        let ValidationError::NoMatch {
            value,
            validator,
            context,
        } = &error
        else {
            panic!("Unexpected diagnostic: {error}");
        };
        assert_eq!(value, &val!("wrong"));
        assert_eq!(
            validator,
            if alternatives.len() == 1 {
                &Validator::Int64
            } else {
                &union
            }
        );
        assert_eq!(context.to_string(), "Path: .items[0].values()");
    }
    let key_validator = Validator::Literal(LiteralValidator::String("allowed".try_into()?));
    let validator = Validator::Array(Box::new(Validator::Record(
        Box::new(key_validator),
        Box::new(Validator::Any),
    )));
    let error = validator
        .check_value(&val!([{"wrong" => true}]), &table_mapping, &virtual_mapping)
        .unwrap_err();
    let ValidationError::LiteralValuesDoNotMatch { context, .. } = error else {
        panic!("Expected a literal mismatch");
    };
    assert_eq!(context.to_string(), "Path: [0].keys()");

    let validator = Validator::Array(Box::new(Validator::Object(object_validator!(
        "required" => FieldValidator::required_field_type(Validator::Int64),
    ))));
    for value in [val!([{}]), val!([{"required" => 1i64, "extra" => true}])] {
        let error = validator
            .check_value(&value, &table_mapping, &virtual_mapping)
            .unwrap_err();
        match error {
            ValidationError::MissingRequiredField { context, .. } => {
                assert_eq!(value, val!([{}]));
                assert_eq!(context.to_string(), "Path: [0]");
            },
            ValidationError::ExtraField { context, .. } => {
                assert_eq!(value, val!([{"required" => 1i64, "extra" => true}]));
                assert_eq!(context.to_string(), "Path: [0]");
            },
            error => panic!("Unexpected diagnostic: {error}"),
        }
    }
    Ok(())
}

#[test]
fn id_union_alternatives_preserve_table_checks() -> anyhow::Result<()> {
    let table_number = TableNumber::try_from(10000u32)?;
    let id = DeveloperDocumentId::new(table_number, InternalId::MIN);
    let value = ConvexValue::from(id);
    let virtual_mapping = VirtualSystemMapping::default();
    let validator = Validator::Id("items".parse()?);
    for name in ["items", "other", "_system"] {
        let mut tables = TableMapping::new();
        tables.insert(
            TabletId::MIN,
            TableNamespace::root_component(),
            table_number,
            name.parse()?,
        );
        let tables = tables.namespace(TableNamespace::root_component());
        let result = validator.check_value(&value, &tables, &virtual_mapping);
        match name {
            "items" => assert_eq!(result, Ok(())),
            "other" => assert!(matches!(
                result,
                Err(ValidationError::TableNamesDoNotMatch { .. })
            )),
            "_system" => assert!(matches!(
                result,
                Err(ValidationError::SystemTableReference { .. })
            )),
            _ => unreachable!(),
        }
        let union = Validator::Union(vec![validator.clone(), Validator::Null]);
        assert_eq!(
            union.check_value(&value, &tables, &virtual_mapping).is_ok(),
            name == "items"
        );
    }
    Ok(())
}

fn nested_tagged_schema(alternatives: usize) -> anyhow::Result<DocumentSchema> {
    let payload = ObjectValidator(
        (0..12)
            .map(|field| {
                Ok((
                    format!("field{field:02}").parse()?,
                    FieldValidator::required_field_type(Validator::Int64),
                ))
            })
            .collect::<anyhow::Result<_>>()?,
    );
    let values = Validator::Array(Box::new(Validator::Object(payload)));
    let nested = Validator::Union(
        (0..8)
            .map(|variant| {
                anyhow::Ok(Validator::Object(object_validator!(
                    "kind" => FieldValidator::required_field_type(Validator::Literal(
                        LiteralValidator::String(format!("nested{variant}").try_into()?),
                    )),
                    "values" => FieldValidator::required_field_type(values.clone()),
                )))
            })
            .collect::<anyhow::Result<_>>()?,
    );
    Ok(DocumentSchema::Union(
        (0..alternatives)
            .map(|variant| {
                anyhow::Ok(object_validator!(
                    "kind" => FieldValidator::required_field_type(Validator::Literal(
                        LiteralValidator::String(format!("variant{variant}").try_into()?),
                    )),
                    "payload" => FieldValidator::required_field_type(nested.clone()),
                ))
            })
            .collect::<anyhow::Result<_>>()?,
    ))
}

fn tagged_document(variant: usize) -> anyhow::Result<ConvexObject> {
    let payload = ConvexObject::try_from(
        (0..12)
            .map(|field| {
                Ok((
                    format!("field{field:02}").parse()?,
                    ConvexValue::Int64(field),
                ))
            })
            .collect::<anyhow::Result<std::collections::BTreeMap<_, _>>>()?,
    )?;
    obj!(
        "_id" => "system field",
        "_creationTime" => 1.0,
        "kind" => format!("variant{variant}"),
        "payload" => {"kind" => "nested7", "values" => [payload.clone(), payload]},
    )
}

#[test]
fn nested_tagged_documents_match_each_alternative() -> anyhow::Result<()> {
    let table_mapping = TableMapping::new().namespace(TableNamespace::root_component());
    let virtual_mapping = VirtualSystemMapping::default();
    let schema = nested_tagged_schema(24)?;
    for variant in 0..24 {
        assert_eq!(
            schema.check_value(&tagged_document(variant)?, &table_mapping, &virtual_mapping),
            Ok(()),
        );
    }
    assert!(schema
        .check_value(&tagged_document(24)?, &table_mapping, &virtual_mapping)
        .is_err());
    assert!(schema
        .check_value(
            &obj!("kind" => "variant23", "payload" => {"kind" => "nested7", "values" => [{"field00" => "wrong"}]})?,
            &table_mapping,
            &virtual_mapping,
        )
        .is_err());
    Ok(())
}

#[test]
#[ignore = "manual timing: run in release mode with --ignored --nocapture"]
fn benchmark_document_schema_validation() -> anyhow::Result<()> {
    let table_mapping = TableMapping::new().namespace(TableNamespace::root_component());
    let virtual_mapping = VirtualSystemMapping::default();
    // Keep this fixture independent of the validation implementation so it can
    // be run unchanged before and after changes to the validation traversal.
    for (name, alternatives, variant) in [("singleton", 1, 0), ("first", 24, 0), ("last", 24, 23)] {
        let schema = nested_tagged_schema(alternatives)?;
        let document = tagged_document(variant)?;
        for _ in 0..100 {
            schema
                .check_value(&document, &table_mapping, &virtual_mapping)
                .unwrap();
        }
        let mut samples = Vec::new();
        for _ in 0..7 {
            let start = Instant::now();
            for _ in 0..2000 {
                black_box(&schema)
                    .check_value(black_box(&document), &table_mapping, &virtual_mapping)
                    .unwrap();
            }
            samples.push(start.elapsed().as_nanos() / 2000);
        }
        samples.sort_unstable();
        println!(
            "{name}: median {} ns/document; samples {samples:?}",
            samples[3]
        );
    }
    Ok(())
}
