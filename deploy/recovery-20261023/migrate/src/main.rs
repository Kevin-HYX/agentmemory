use rkyv::{Archive, Deserialize, Serialize};
use serde_json::{Value, Map, json};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};
use std::io::{BufWriter, Write};

#[derive(Archive, Serialize, Deserialize)]
struct KeyStorage(String);

fn read(path: &Path) -> Map<String, Value> {
    let bytes = fs::read(path).unwrap();
    let value = rkyv::from_bytes::<KeyStorage, rkyv::rancor::Error>(&bytes).unwrap();
    serde_json::from_str(&value.0).unwrap()
}

fn write(output: &Path, scope: &str, rows: &Map<String, Value>) -> usize {
    let filename = scope.replace(":", "%3A") + ".bin";
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&KeyStorage(serde_json::to_string(rows).unwrap())).unwrap();
    fs::write(output.join(&filename), &bytes).unwrap();
    assert_eq!(*rows, read(&output.join(&filename)), "迁移后逐记录复读不一致");
    bytes.len()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args[1] == "export-jsonl" {
        let mut output = BufWriter::new(std::io::stdout().lock());
        let source=Path::new(&args[2]);
        let mut files: Vec<_> = fs::read_dir(source).unwrap().map(|e|e.unwrap().path()).filter(|p|p.extension().and_then(|x|x.to_str())==Some("bin")).collect();
        files.sort();
        for path in files {
            let scope=percent_encoding::percent_decode_str(path.file_stem().unwrap().to_str().unwrap()).decode_utf8().unwrap().into_owned();
            for (key,value) in read(&path) {
                serde_json::to_writer(&mut output,&json!([scope,key,value])).unwrap();
                output.write_all(b"\n").unwrap();
            }
        }
        output.flush().unwrap(); return;
    }
    let source = Path::new(&args[1]);
    let output = Path::new(&args[2]);
    fs::create_dir(output).expect("目标必须不存在，避免覆盖数据");
    let mut summary = Map::new();
    let scopes = ["mem:graph:nodes", "mem:graph:edges", "mem:insights", "mem:semantic"];
    for entry in fs::read_dir(source).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|x| x.to_str()) != Some("bin") { continue; }
        let name = path.file_stem().unwrap().to_str().unwrap();
        let decoded = percent_encoding::percent_decode_str(name).decode_utf8().unwrap();
        if decoded.starts_with("patch:v1:") {
            assert!(scopes.iter().any(|scope| decoded.starts_with(&format!("patch:v1:{}:",scope))), "未知分片范围，拒绝跳过");
            continue;
        }
        let rows = read(&path);
        assert!(!scopes.contains(&decoded.as_ref()) || rows.is_empty(), "原生与分片数据同时存在，必须人工核对");
        fs::copy(&path, output.join(path.file_name().unwrap())).unwrap();
    }
    let mut layouts = Map::new();
    for scope in scopes {
        let mut merged = Map::new();
        let mut input_digest = Sha256::new();
        let mut max_record = 0;
        let mut field_bytes: std::collections::BTreeMap<String, usize> = Default::default();
        for shard in 0..256 {
            let name = format!("patch:v1:{}:{:02x}", scope, shard).replace(":", "%3A") + ".bin";
            let path = source.join(name);
            if !path.exists() { continue; }
            let mut bucket = read(&path);
            assert_eq!(bucket.len(), 1, "非预期分片键");
            let rows = bucket.remove("rows").unwrap().as_object().unwrap().clone();
            for (key, value) in rows {
                assert_eq!(Sha256::digest(key.as_bytes())[0], shard as u8);
                let raw = serde_json::to_vec(&value).unwrap();
                input_digest.update(key.as_bytes()); input_digest.update(&raw);
                max_record = max_record.max(raw.len());
                if let Some(fields) = value.as_object() {
                    for (k,v) in fields { *field_bytes.entry(k.clone()).or_default() += serde_json::to_vec(v).unwrap().len(); }
                }
                assert!(merged.insert(key, value).is_none(), "发现重复键");
            }
        }
        let count = merged.len();
        let mut partitions: Vec<Map<String, Value>> = (0..1024).map(|_| Map::new()).collect();
        for (key,value) in merged {
            let hash=Sha256::digest(key.as_bytes());
            let index=u16::from_be_bytes([hash[0],hash[1]]) as usize % 1024;
            partitions[index].insert(key,value);
        }
        let mut byte_count=0; let mut max_partition=0; let mut output_digest=Sha256::new();
        for (index,rows) in partitions.iter().enumerate() {
            let scope_name=format!("partition:v2:{}:{:03x}",scope,index);
            let size=write(output,&scope_name,rows);
            output_digest.update(serde_json::to_vec(rows).unwrap());
            byte_count+=size; max_partition=max_partition.max(size);
        }
        layouts.insert(scope.to_string(),json!({"version":2,"partitions":1024}));
        summary.insert(scope.to_string(), json!({"records":count,"bytes":byte_count,"largestPartitionBytes":max_partition,"largestRecordBytes":max_record,"fieldBytes":field_bytes,"inputRecordDigest":format!("{:x}",input_digest.finalize()),"outputRecordDigest":format!("{:x}",output_digest.finalize()),"readbackEqual":true}));
        println!("{}", json!({scope:summary[scope]}));
    }
    write(output,"mem:storage:layouts",&layouts);
    fs::write(output.join("migration-report.json"), serde_json::to_vec_pretty(&summary).unwrap()).unwrap();
}
