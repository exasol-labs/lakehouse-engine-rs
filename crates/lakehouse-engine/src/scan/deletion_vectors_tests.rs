use super::*;
use crate::scan::spec::DeltaDeletionVectorStorage;

/// Real Delta writer output for the `table-with-dv-small` fixture, not hand-assembled bytes.
const SIDECAR_BODY: &[u8] = include_bytes!(
    "../../../../scripts/unity/fixtures/table-with-dv-small/deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin"
);
const LOGGED_PATH: &str = "vBn[lx{q8@P<9BNH/isA";
const LOGGED_SIZE_IN_BYTES: i32 = 36;
const LOGGED_CARDINALITY: i64 = 2;

/// The sidecar body without its container framing, Z85-encoded, as an inline descriptor carries it.
const INLINE_PAYLOAD: &str = "^Bg9^0rr910000000000iXQKl0rr91000315c8Xg000r9";

const TABLE_ROOT: &str = "s3://bucket/db/table_with_dv";
const SIDECAR_URL: &str =
    "s3://bucket/db/table_with_dv/deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";
const SECRET: &str = "SECRETKEY";

fn data_file() -> String {
    format!("s3://{SECRET}@bucket/db/table_with_dv/part-00000.snappy.parquet")
}

fn secrets() -> Vec<String> {
    vec![SECRET.to_string()]
}

fn logged(
    storage: DeltaDeletionVectorStorage,
    path_or_inline_dv: &str,
) -> LoggedDeletionVector<'_> {
    LoggedDeletionVector {
        storage,
        path_or_inline_dv,
        offset: match storage {
            DeltaDeletionVectorStorage::Inline => None,
            _ => Some(1),
        },
        size_in_bytes: LOGGED_SIZE_IN_BYTES,
        cardinality: LOGGED_CARDINALITY,
    }
}

fn resolve(
    storage: DeltaDeletionVectorStorage,
    path_or_inline_dv: &str,
) -> Result<DeltaDeletionVector, UdfError> {
    DeltaDeletionVector::resolve(
        logged(storage, path_or_inline_dv),
        TABLE_ROOT,
        &data_file(),
        &secrets(),
    )
}

fn decode_sidecar(body: Vec<u8>) -> Result<RoaringTreemap, UdfError> {
    resolve(DeltaDeletionVectorStorage::UuidRelative, LOGGED_PATH)
        .unwrap()
        .decode(Some(Bytes::from(body)), &data_file(), &secrets())
}

fn body_with_byte(index: usize, value: u8) -> Vec<u8> {
    let mut body = SIDECAR_BODY.to_vec();
    body[index] = value;
    body
}

/// Every refusal names the data file, carries nothing secret, and never echoes an inline payload.
fn assert_clean_refusal<T>(result: Result<T, UdfError>, context: &str) -> String {
    let Err(err) = result else {
        panic!("{context} must be refused");
    };
    let err = err.to_string();
    assert!(
        err.contains("part-00000.snappy.parquet"),
        "{context}: names the affected data file: {err}"
    );
    assert!(
        !err.contains(SECRET),
        "{context}: must not leak credentials: {err}"
    );
    assert!(
        !err.contains(INLINE_PAYLOAD),
        "{context}: must not echo an inline payload: {err}"
    );
    err
}

#[test]
fn sidecar_path_is_resolved_once_for_each_storage_kind() {
    let uuid_relative = resolve(DeltaDeletionVectorStorage::UuidRelative, LOGGED_PATH).unwrap();
    assert_eq!(
        uuid_relative.sidecar_url().map(Url::as_str),
        Some(SIDECAR_URL),
        "a UUID-relative vector names its sidecar under the table root"
    );

    let absolute = "s3://other-bucket/elsewhere/dv-file.bin";
    let absolute_path = resolve(DeltaDeletionVectorStorage::AbsolutePath, absolute).unwrap();
    assert_eq!(
        absolute_path.sidecar_url().map(Url::as_str),
        Some(absolute),
        "an absolute-path vector is read verbatim, with no table root joined on"
    );

    let inline = resolve(DeltaDeletionVectorStorage::Inline, INLINE_PAYLOAD).unwrap();
    assert_eq!(
        inline.sidecar_url(),
        None,
        "an inline vector resolves no path, so the scan fetches nothing for it"
    );
}

#[test]
fn storage_shim_serves_prefetched_bytes_and_refuses_every_other_operation() {
    let served = Url::parse(SIDECAR_URL).unwrap();
    let elsewhere = Url::parse("s3://bucket/db/table_with_dv/never-fetched.bin").unwrap();
    let shim = PrefetchedDeletionVectorBytes::holding(served.clone(), Bytes::from(SIDECAR_BODY));
    let read_one = |slice| shim.read_files(vec![slice]).unwrap().next().unwrap();

    assert_eq!(
        read_one((served.clone(), None)).unwrap().as_ref(),
        SIDECAR_BODY,
        "a read of the fetched body yields it verbatim"
    );
    assert_eq!(
        read_one((served.clone(), Some(1..5))).unwrap().as_ref(),
        &SIDECAR_BODY[1..5],
        "a ranged read is served from that same fetched body"
    );
    assert!(
        read_one((served.clone(), Some(0..u64::MAX))).is_err(),
        "a range beyond the fetched body is refused rather than panicked on"
    );
    assert!(
        read_one((elsewhere.clone(), None)).is_err(),
        "reading a body the scan never fetched is refused, never fabricated"
    );

    let refusals = [
        ("list_from", shim.list_from(&served).err().is_some()),
        (
            "copy_atomic",
            shim.copy_atomic(&served, &elsewhere).is_err(),
        ),
        (
            "put",
            shim.put(&served, Bytes::from_static(b"x"), true).is_err(),
        ),
        ("head", shim.head(&served).is_err()),
        ("delete", shim.delete(&served).is_err()),
    ];
    for (operation, refused) in refusals {
        assert!(refused, "{operation} must be refused, not performed");
    }
    assert!(
        shim.head(&served).unwrap_err().to_string().contains("head"),
        "a refusal names the unsupported operation"
    );

    let positions = resolve(DeltaDeletionVectorStorage::UuidRelative, LOGGED_PATH)
        .unwrap()
        .decode(Some(Bytes::from(SIDECAR_BODY)), &data_file(), &secrets())
        .unwrap();
    assert_eq!(
        positions.len(),
        LOGGED_CARDINALITY as u64,
        "decoding through the shim yields the vector the log describes"
    );
}

#[test]
fn inline_vector_decodes_from_its_payload_without_any_prefetched_bytes() {
    let inline = resolve(DeltaDeletionVectorStorage::Inline, INLINE_PAYLOAD)
        .unwrap()
        .decode(None, &data_file(), &secrets())
        .unwrap();
    assert_eq!(
        inline,
        decode_sidecar(SIDECAR_BODY.to_vec()).unwrap(),
        "the same bitmap payload yields the same positions under either storage kind"
    );
}

#[test]
fn untrusted_deletion_vector_containers_fail_loud_without_panicking() {
    assert_clean_refusal(
        decode_sidecar(body_with_byte(0, 2)),
        "a container version other than 1",
    );
    assert_clean_refusal(
        decode_sidecar(body_with_byte(4, 0x23)),
        "a stored size the log contradicts",
    );
    assert_clean_refusal(
        decode_sidecar(body_with_byte(5, 0x00)),
        "a foreign bitmap magic",
    );
    assert_clean_refusal(decode_sidecar(body_with_byte(44, 0x00)), "a broken CRC-32");
    assert_clean_refusal(
        decode_sidecar(SIDECAR_BODY[..20].to_vec()),
        "a container truncated before its checksum",
    );
    assert_clean_refusal(
        resolve(DeltaDeletionVectorStorage::UuidRelative, LOGGED_PATH)
            .unwrap()
            .decode(None, &data_file(), &secrets()),
        "a sidecar vector whose body was never fetched",
    );
    assert_clean_refusal(
        resolve(DeltaDeletionVectorStorage::Inline, "not valid z85 at all")
            .unwrap()
            .decode(None, &data_file(), &secrets()),
        "an inline payload that does not decode",
    );
}

#[test]
fn descriptors_the_protocol_forbids_are_refused_before_any_fetch() {
    for payload in ["", "0", "01234"] {
        assert_clean_refusal(
            resolve(DeltaDeletionVectorStorage::Inline, payload),
            "an inline payload too short to hold the portable magic",
        );
    }

    let short_size = DeltaDeletionVector::resolve(
        LoggedDeletionVector {
            size_in_bytes: 3,
            ..logged(DeltaDeletionVectorStorage::UuidRelative, LOGGED_PATH)
        },
        TABLE_ROOT,
        &data_file(),
        &secrets(),
    );
    assert_clean_refusal(
        short_size,
        "a persisted size smaller than the portable magic",
    );

    assert_clean_refusal(
        resolve(DeltaDeletionVectorStorage::UuidRelative, "too-short"),
        "a UUID-relative path with no Z85 UUID",
    );
    assert_clean_refusal(
        DeltaDeletionVector::resolve(
            LoggedDeletionVector {
                cardinality: -1,
                ..logged(DeltaDeletionVectorStorage::UuidRelative, LOGGED_PATH)
            },
            TABLE_ROOT,
            &data_file(),
            &secrets(),
        ),
        "a negative cardinality",
    );
}

#[test]
fn decoded_set_disagreeing_with_the_declared_cardinality_fails_the_scan() {
    let overstated = DeltaDeletionVector::resolve(
        LoggedDeletionVector {
            cardinality: LOGGED_CARDINALITY + 1,
            ..logged(DeltaDeletionVectorStorage::UuidRelative, LOGGED_PATH)
        },
        TABLE_ROOT,
        &data_file(),
        &secrets(),
    )
    .unwrap()
    .decode(Some(Bytes::from(SIDECAR_BODY)), &data_file(), &secrets());

    let err = assert_clean_refusal(overstated, "a set the declared cardinality contradicts");
    assert!(
        err.contains("cardinality"),
        "the refusal states which validation failed: {err}"
    );
}

/// Delta and Iceberg share the blob layout, so the fixture sidecar's one vector is a valid
/// `deletion-vector-v1` blob at the logged offset, spanning size, magic, bitmap, and CRC.
const PUFFIN_BLOB_RANGE: Range<u64> = 1..45;

fn puffin_vector(
    file_size: u64,
    offset: u64,
    length: u64,
    cardinality: u64,
) -> Result<IcebergDeletionVector<'static>, UdfError> {
    IcebergDeletionVector::resolve(
        ManifestDeletionVector {
            path: "dv.puffin",
            size: file_size,
            content_offset: offset,
            content_size_in_bytes: length,
            record_count: cardinality,
        },
        &data_file(),
        &secrets(),
    )
}

fn decode_puffin_blob(blob: &[u8], cardinality: u64) -> Result<RoaringTreemap, UdfError> {
    let length = blob.len() as u64;
    puffin_vector(length, 0, length, cardinality)
        .unwrap()
        .decode(blob, &data_file(), &secrets())
}

#[test]
fn puffin_blob_decodes_to_the_same_positions_as_its_delta_twin() {
    let range = PUFFIN_BLOB_RANGE;
    let blob = &SIDECAR_BODY[range.start as usize..range.end as usize];
    let vector = puffin_vector(
        SIDECAR_BODY.len() as u64,
        range.start,
        range.end - range.start,
        2,
    )
    .unwrap();
    assert_eq!(vector.range, range);

    assert_eq!(
        vector.decode(blob, &data_file(), &secrets()).unwrap(),
        decode_sidecar(SIDECAR_BODY.to_vec()).unwrap()
    );
}

/// Scenario: An Iceberg deletion vector that cannot be applied fails the scan
#[test]
fn untrusted_puffin_deletion_vectors_fail_loud_without_panicking() {
    let blob = &SIDECAR_BODY[PUFFIN_BLOB_RANGE.start as usize..PUFFIN_BLOB_RANGE.end as usize];
    let with_byte = |index: usize, value: u8| {
        let mut bytes = blob.to_vec();
        bytes[index] = value;
        bytes
    };

    assert_clean_refusal(puffin_vector(44, 1, 44, 2), "a blob reaching past the file");
    assert_clean_refusal(
        puffin_vector(44, u64::MAX, 2, 2),
        "a blob range that overflows",
    );
    assert_clean_refusal(
        decode_puffin_blob(&with_byte(3, 0x23), 2),
        "a length prefix the blob contradicts",
    );
    assert_clean_refusal(
        decode_puffin_blob(&with_byte(4, 0x00), 2),
        "a foreign magic",
    );
    assert_clean_refusal(
        decode_puffin_blob(&with_byte(43, 0x00), 2),
        "a broken CRC-32",
    );
    assert_clean_refusal(decode_puffin_blob(&blob[..20], 2), "a truncated blob");
    let err = assert_clean_refusal(
        decode_puffin_blob(blob, 3),
        "a decoded set disagreeing with the manifest cardinality",
    );
    assert!(err.contains("Iceberg deletion vector"), "{err}");
}
