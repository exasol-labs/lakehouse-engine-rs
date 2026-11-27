use super::*;
use crate::test_support::{STATIC_SECRETS, ident, static_storage};
use lakehouse_catalog::AdlsCred;

fn azure_storage() -> WriteStorage {
    WriteStorage::Static(StorageBackend::Adls {
        account_name: "account".into(),
        cred: AdlsCred::AccountKey("c2VjcmV0LWtleQ==".into()),
    })
}

fn vended_storage() -> WriteStorage {
    WriteStorage::Vended(StaticStoreAddress::default())
}

#[test]
fn a_location_its_storage_cannot_address_is_refused_naming_the_table_and_the_scheme() {
    let abfss = "abfss://warehouse@account.dfs.core.windows.net/ns/t";
    let abfs = "abfs://warehouse@account.dfs.core.windows.net/ns/t";
    let abfs_needs_allow_http = "plaintext scheme abfs needs the export option ALLOW_HTTP = \
                                 'true', which acknowledges that the export writes the location \
                                 over HTTPS";
    // (case, storage, ALLOW_HTTP, location, the vended backend it selects or the refusal's tail)
    type Case<'a> = (
        &'a str,
        WriteStorage,
        bool,
        &'a str,
        Result<&'a str, &'a str>,
    );
    let cases: [Case; 10] = [
        (
            "static S3 on abfss",
            static_storage(),
            false,
            abfss,
            Err("scheme abfss the CONNECTION's S3 storage cannot address"),
        ),
        (
            "static S3 on s3a",
            static_storage(),
            false,
            "s3a://warehouse/ns/t",
            Ok("none"),
        ),
        (
            "static Azure on s3",
            azure_storage(),
            true,
            "s3://warehouse/ns/t",
            Err("scheme s3 the CONNECTION's Azure storage cannot address"),
        ),
        (
            "static Azure on abfs",
            azure_storage(),
            true,
            abfs,
            Err("scheme abfs the CONNECTION's Azure storage cannot address"),
        ),
        (
            "static Azure on abfss",
            azure_storage(),
            false,
            abfss,
            Ok("none"),
        ),
        (
            "vended on gs",
            vended_storage(),
            true,
            "gs://warehouse/ns/t",
            Err("scheme gs vended storage cannot address"),
        ),
        (
            "vended on abfs without ALLOW_HTTP",
            vended_storage(),
            false,
            abfs,
            Err(abfs_needs_allow_http),
        ),
        (
            "vended on abfs with ALLOW_HTTP",
            vended_storage(),
            true,
            abfs,
            Ok("ADLS"),
        ),
        (
            "vended on abfss",
            vended_storage(),
            false,
            abfss,
            Ok("ADLS"),
        ),
        (
            "vended on s3a",
            vended_storage(),
            false,
            "s3a://warehouse/ns/t",
            Ok("S3"),
        ),
    ];

    for (case, storage, allow_http, location, expected) in cases {
        let checked = refuse_unaddressable(&storage, allow_http, &ident("ns", "t"), location)
            .map(|kind| match kind {
                None => "none",
                Some(VendedBackendKind::S3) => "S3",
                Some(VendedBackendKind::Adls) => "ADLS",
            })
            .map_err(|error| error.to_string());

        let expected = expected
            .map_err(|tail| format!("table ns.t has the location {location}, whose {tail}"));
        assert_eq!(checked, expected, "{case}");
    }
}

#[test]
fn a_connection_that_vends_gets_vended_storage_and_one_with_keys_its_own() {
    let mut creds = ConnectionCreds {
        endpoint: "https://storage.example".into(),
        access_key: STATIC_SECRETS[0].into(),
        secret_key: STATIC_SECRETS[1].into(),
        ..Default::default()
    };

    let own = WriteStorage::of(&creds, false, None);
    creds.use_vended_credentials = true;
    let vended = WriteStorage::of(&creds, false, None);

    let WriteStorage::Static(backend) = own else {
        panic!("a CONNECTION with keys writes with them");
    };
    assert_eq!(backend.secret_values(), STATIC_SECRETS);
    let WriteStorage::Vended(address) = vended else {
        panic!("a vending CONNECTION writes with vended credentials");
    };
    assert_eq!(address.endpoint(), "https://storage.example");
}
