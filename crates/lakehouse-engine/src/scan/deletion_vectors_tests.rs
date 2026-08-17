use super::*;
use crate::scan::spec::DeltaDeletionVectorStorage;

/// The 45-byte deletion-vector container Delta wrote for the vendored
/// `table-with-dv-small` fixture, plus the `add.deletionVector` descriptor its commit
/// logs for it. Real writer output rather than hand-assembled bytes, so the version
/// byte, the big-endian size, the portable magic, and the CRC-32 are the ones a Delta
/// engine actually produces.
const SIDECAR_BODY: &[u8] = include_bytes!(
    "../../../../scripts/unity/fixtures/table-with-dv-small/deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin"
);
const LOGGED_PATH: &str = "vBn[lx{q8@P<9BNH/isA";
const LOGGED_SIZE_IN_BYTES: i32 = 36;
const LOGGED_CARDINALITY: i64 = 2;

/// That same vector's `magicNumber ++ bitmapData` — the sidecar body without its
/// container framing — Z85-encoded, which is exactly what an inline descriptor carries.
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
) -> Result<DeletionVector, UdfError> {
    DeletionVector::resolve(
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

/// Assert a refusal names the affected data file, carries nothing secret, and never
/// echoes an inline payload — the guarantees every deletion-vector error owes the
/// scan, whichever validation produced it.
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

/// The three Delta storage kinds resolve to where their bytes live exactly once: a
/// UUID-relative descriptor reconstructs `<table root>/deletion_vector_<uuid>.bin`, an
/// absolute one is taken verbatim, and an inline one resolves no path at all.
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

/// The decoder is handed bytes and never a live storage client: the shim answers a read
/// of an already-fetched body and refuses every other operation with an error rather
/// than a panic, which inside a UDF would be an abnormal VM exit.
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

/// An inline vector's bytes are the descriptor itself, so it decodes to the same
/// positions the sidecar carries without any fetched body being supplied.
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

/// Every container the scan cannot trust — a wrong version byte, a size the log
/// contradicts, a foreign magic, a broken checksum, a truncated container, a payload
/// that does not decode, and bytes that were never fetched — fails as an error value
/// before any row is emitted, and never as a panic.
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

/// A descriptor whose own fields the Delta protocol forbids is refused at resolution,
/// before the scan fetches anything for it — including the two shapes whose bytes the
/// decoder would index unguarded: an inline payload too short to hold the portable
/// magic, and a persisted size smaller than that magic.
#[test]
fn descriptors_the_protocol_forbids_are_refused_before_any_fetch() {
    for payload in ["", "0", "01234"] {
        assert_clean_refusal(
            resolve(DeltaDeletionVectorStorage::Inline, payload),
            "an inline payload too short to hold the portable magic",
        );
    }

    let short_size = DeletionVector::resolve(
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
        DeletionVector::resolve(
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

/// A decoded set the log's own cardinality contradicts is a row set the scan cannot
/// trust, so it fails rather than emitting rows the table may have deleted.
#[test]
fn decoded_set_disagreeing_with_the_declared_cardinality_fails_the_scan() {
    let overstated = DeletionVector::resolve(
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
