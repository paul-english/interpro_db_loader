use std::fs::File;
use std::time::{Instant, Duration};
use std::io::BufReader;
use tokio_postgres::Transaction;
use xml::reader::{EventReader, XmlEvent};
use std::collections::HashMap;
use std::sync::Arc;
use std::io::{self, Write};

use tokio_postgres::{NoTls, Client, Config, Connection, Socket, tls::{NoTlsStream}};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::{mpsc, Mutex, Semaphore};

async fn build_db(arc_client: &Arc<Mutex<Client>>) -> Result<(), Box<dyn std::error::Error>> {
    let client = arc_client.lock().await;

    println!("Drop tables if exists");
    let stmt = client.prepare("DROP TABLE IF EXISTS dbinfo").await?;
    client.execute(&stmt, &[]).await?;
    let stmt = client.prepare("DROP TABLE IF EXISTS lcn").await?;
    client.execute(&stmt, &[]).await?;
    let stmt = client.prepare("DROP TABLE IF EXISTS ipr").await?;
    client.execute(&stmt, &[]).await?;
    let stmt = client.prepare("DROP TABLE IF EXISTS match").await?;
    client.execute(&stmt, &[]).await?;
    let stmt = client.prepare("DROP TABLE IF EXISTS protein").await?;
    client.execute(&stmt, &[]).await?;

    println!("Create dbinfo");
    let stmt = client.prepare("
        CREATE TABLE dbinfo (
            dbinfo_id SERIAL PRIMARY KEY,
            dbname TEXT NOT NULL UNIQUE,
            version TEXT,
            entry_count integer,
            file_date TEXT
        )
    ").await?;
    client.execute(&stmt, &[]).await?;

    println!("Create protein");
    let stmt = client.prepare("
        CREATE TABLE protein (
            protein_id SERIAL PRIMARY KEY,
            id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            length integer NOT NULL,
            crc64 TEXT NOT NULL
        )
    ").await?;
    client.execute(&stmt, &[]).await?;

    println!("Create match");
    let stmt = client.prepare("
        CREATE TABLE match (
            match_id SERIAL PRIMARY KEY,
            protein_id INTEGER NOT NULL,
            id TEXT NOT NULL,
            name TEXT NOT NULL,
            dbname TEXT NOT NULL,
            status TEXT NOT NULL,
            evd TEXT NOT NULL,
            model TEXT NOT NULL,
            FOREIGN KEY (protein_id) REFERENCES protein (protein_id)
        )
    ").await?;
    client.execute(&stmt, &[]).await?;

    println!("Create lcn");
    let stmt = client.prepare("
        CREATE TABLE lcn (
            lcn_id SERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL,
            start integer NOT NULL,
            \"end\" integer NOT NULL,
            fragments TEXT NOT NULL,
            score TEXT NOT NULL,
            FOREIGN KEY (match_id) REFERENCES match (match_id)
        )
    ").await?;
    client.execute(&stmt, &[]).await?;

    println!("Create ipr");
    let stmt = client.prepare("
        CREATE TABLE ipr (
            ipr_id SERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL,
            id TEXT NOT NULL,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            FOREIGN KEY (match_id) REFERENCES match (match_id)
        )
    ").await?;
    client.execute(&stmt, &[]).await?;

    Ok(())
}

async fn process_dbinfo(arc_client: &Arc<Mutex<Client>>, dbinfo: DbInfo) -> Result<(), Box<dyn std::error::Error>> {
    let client = arc_client.lock().await;
    let stmt = client.prepare("INSERT INTO dbinfo (dbname, version, entry_count, file_date) VALUES ($1, $2, $3, $4)").await?;
    client.execute(&stmt, &[
        &dbinfo.dbname,
        &dbinfo.version,
        &dbinfo.entry_count.parse::<i32>().unwrap(),
        &dbinfo.file_date
    ]).await?;
    Ok(())
}
async fn process_protein(arc_client: &Arc<Mutex<Client>>, protein: Protein) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = arc_client.lock().await;
    let transaction = client.transaction().await?;
    let attrs_map: HashMap<_, _> = protein.attrs.into_iter().collect();
    //println!("protein attrs: {:?}", attrs_map);
    let stmt = transaction.prepare("INSERT INTO protein (id, name, length, crc64) VALUES ($1, $2, $3, $4) RETURNING protein_id").await?;
    let length: &String = attrs_map.get("length").unwrap();
    let protein_id: i32 = transaction.query_one(&stmt, &[
        attrs_map.get("id").unwrap(),
        attrs_map.get("name").unwrap(),
        &length.parse::<i32>().unwrap(),
        attrs_map.get("crc64").unwrap()
    ]).await?.get(0);

    for m in protein.matches {
        //println!("Processing match: {:?}", m);
        process_match(&transaction, protein_id, m).await?;
    }
    transaction.commit().await?;

    Ok(())

}

async fn process_match(transaction: &Transaction<'_>, protein_id: i32, m: Match) -> Result<(), Box<dyn std::error::Error>> {
    let attrs_map: HashMap<_, _> = m.attrs.into_iter().collect();
    let stmt = transaction.prepare("INSERT INTO match (protein_id, id, name, dbname, status, evd, model) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING match_id").await?;
    let match_id: i32 = transaction.query_one(&stmt, &[
        &protein_id,
        attrs_map.get("id").unwrap(),
        attrs_map.get("name").unwrap(),
        attrs_map.get("dbname").unwrap(),
        attrs_map.get("status").unwrap(),
        attrs_map.get("evd").unwrap(),
        attrs_map.get("model").unwrap()
    ]).await?.get(0);

    process_lcn(transaction, match_id, m.lcn).await?;
    if let Some(i) = m.ipr {
        process_ipr(transaction, match_id, i).await?;
    }

    Ok(())
}

async fn process_lcn(transaction: &Transaction<'_>, match_id: i32, l: Lcn) -> Result<(), Box<dyn std::error::Error>> {
    let attrs_map: HashMap<_, _> = l.attrs.into_iter().collect();
    let stmt = transaction.prepare("INSERT INTO lcn (match_id, start, \"end\", fragments, score) VALUES ($1, $2, $3, $4, $5)").await?;
    let start: &String = attrs_map.get("start").unwrap();
    let end: &String = attrs_map.get("end").unwrap();
    transaction.execute(&stmt, &[
        &match_id,
        &start.parse::<i32>().unwrap(),
        &end.parse::<i32>().unwrap(),
        attrs_map.get("fragments").unwrap(),
        attrs_map.get("score").unwrap()
    ]).await?;
    Ok(())
}

async fn process_ipr(transaction: &Transaction<'_>, match_id: i32, i: Ipr) -> Result<(), Box<dyn std::error::Error>> {
    let attrs_map: HashMap<_, _> = i.attrs.into_iter().collect();
    let stmt = transaction.prepare("INSERT INTO ipr (match_id, id, name, type) VALUES ($1, $2, $3, $4)").await?;
    transaction.execute(&stmt, &[
        &match_id,
        attrs_map.get("id").unwrap(),
        attrs_map.get("name").unwrap(),
        attrs_map.get("type").unwrap(),
    ]).await?;
    Ok(())
}

async fn get_db() -> (Client, Connection<Socket, NoTlsStream>) {
    Config::new()
        .host("localhost")
        .user("postgres")
        .port(5432)
        .password("asdf1234")
        .dbname("interpro")
        .connect(NoTls)
        .await
        .unwrap()
}

struct DbInfo {
    dbname: String,
    version: String,
    entry_count: String,
    file_date: String,
}

#[derive(Debug)]
struct Protein {
    matches: Vec<Match>,
    attrs: Vec<(String, String)>,
}

#[derive(Debug)]
struct Match {
    ipr: Option<Ipr>,
    lcn: Lcn,
    attrs: Vec<(String, String)>,
}

#[derive(Debug)]
struct Ipr {
    attrs: Vec<(String, String)>,
}

#[derive(Debug)]
struct Lcn {
    attrs: Vec<(String, String)>,
}

async fn get_protein_ids(arc_client: &Arc<Mutex<Client>>) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let client = arc_client.lock().await;
    let results = client.query("SELECT DISTINCT id FROM protein", &[]).await?;
    let ids: Vec<String> = results.iter().map(|row| row.get(0)).collect();
    Ok(ids)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (db_client, connection) = get_db().await;
    let client = Arc::new(Mutex::new(db_client));

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // ask user if they want to reset the db
    print!("Do you want to reset the database? (y/N): ");
    io::stdout().flush()?; // Make sure the question is displayed before getting the user's input
    
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    match input.trim().to_lowercase().as_str() {
        "y" | "yes" => {
            // If the user answered "yes", reset the database
            build_db(&client).await?;
            println!("Database reset successful.");
        }
        _ => {
            // If the user answered anything else, don't reset the database
            println!("Database reset skipped.");
        }
    }

    // fetch all the distinct protein.id's
    let loaded_protein_ids = get_protein_ids(&client).await?;
    println!("Loaded known {} protein ids", loaded_protein_ids.len());

    // Use that cached list to skip already loaded records

    let file = File::open("/mnt/md0/data/interpro/match_complete.xml").unwrap();
    let file = BufReader::new(file);
    let parser = EventReader::new(file);

    let mut stack: Vec<(String, Vec<(String, String)>, Vec<XmlEvent>)> = Vec::new();
    
    let mut i = 0;
    
    let bar = ProgressBar::new_spinner();
    bar.enable_steady_tick(Duration::new(0, 100000000));
    let style = ProgressStyle::default_spinner()
        .template("{spinner} {wide_msg}")?;
    bar.set_style(style);

    let start_time = Instant::now();
    let mut msg = String::new();

    let (tx, mut rx) = mpsc::channel(256);
    let semaphore = Arc::new(Semaphore::new(256));

    let mut skip_this_protein = false;

    for e in parser {
        let iteration_start_time = Instant::now();

        match e {
            Ok(XmlEvent::StartElement { name, attributes, .. }) => {
                if name.local_name == "dbinfo" {
                    let attrs_map: HashMap<_, _> = attributes.into_iter().map(|a| (a.name.local_name, a.value)).collect();
                    process_dbinfo(&client, DbInfo { 
                        dbname: attrs_map.get("dbname").unwrap().clone(), 
                        version: attrs_map.get("version").unwrap().clone(),
                        entry_count: attrs_map.get("entry_count").unwrap().clone(),
                        file_date: attrs_map.get("file_date").unwrap().clone(), 
                    }).await?;
                } else if name.local_name.as_str() == "protein" {
                        let attrs: Vec<(String, String)> = attributes.into_iter().map(|a| (a.name.local_name, a.value)).collect();
                        attrs.iter().for_each(|(name, val)| {
                            if name == "id" {
                                if loaded_protein_ids.contains(&val) {
                                    // skip until the next protein
                                    //println!("skipping protein {}", a.value);
                                    skip_this_protein = true;
                                } else {
                                    skip_this_protein = false;
                                }
                            }
                        });
                        if skip_this_protein {

                            i += 1;
                            if i % 100 == 0 {

                                let iteration_duration = iteration_start_time.elapsed();
                                let total_elapsed = start_time.elapsed();
                                msg = format!(
                                    "Iteration: {}, Iteration Time: {:.2?}, Total Time: {:.2?}",
                                    i,
                                    iteration_duration,
                                    total_elapsed
                                );
                                bar.inc(100);
                                bar.set_message(msg);
                            }

                            continue;
                        } else {
                            stack.push((
                                name.local_name, 
                                attrs, 
                                Vec::new()
                            ));

                        }
                } else if ["match", "ipr", "lcn"].contains(&&name.local_name.as_str()) {
                    if skip_this_protein {
                        continue
                    }
                    //println!("start element: {:?}", name);
                    stack.push((
                        name.local_name, 
                        attributes.into_iter().map(|a| (a.name.local_name, a.value)).collect(), 
                        Vec::new()
                    ));
                } else {
                    println!("unhandled start element: {:?}", name);
                }
            }
            Ok(XmlEvent::Characters(_content)) => {
                unimplemented!("characters"); // File shouldn't contain text in the xml elements
            }
            Ok(XmlEvent::EndElement { name: _end_name }) => {
                //println!("end element: {:?}", end_name.local_name);
                if let Some((name, attrs, children)) = stack.pop() {
                    if stack.is_empty() && (name == "protein") {
                        //println!("process: {} {:?} {:?}", name, attrs, children);
                        let mut protein = Protein {
                            matches: Vec::new(),
                            attrs,
                        };
                        for c in children {
                            match c {
                                XmlEvent::StartElement { name, attributes, .. } => {
                                    if name.local_name == "match" {
                                        let attrs: Vec<(_, _)> = attributes.into_iter().map(|a| (a.name.local_name, a.value)).collect();
                                        let m = Match {
                                            ipr: None,
                                            lcn: Lcn { attrs: Vec::new() },
                                            attrs: attrs,
                                        };
                                        protein.matches.push(m);
                                    } else if name.local_name == "ipr" {
                                        let attrs: Vec<(_, _)> = attributes.into_iter().map(|a| (a.name.local_name, a.value)).collect();
                                        let ipr = Ipr { attrs };
                                        protein.matches.last_mut().unwrap().ipr = Some(ipr);
                                    } else if name.local_name == "lcn" {
                                        let attrs: Vec<(_, _)> = attributes.into_iter().map(|a| (a.name.local_name, a.value)).collect();
                                        let lcn = Lcn { attrs };
                                        protein.matches.last_mut().unwrap().lcn = lcn;
                                    }
                                }
                                _ => {
                                    // Not needed
                                }
                            }
                        }
                        //println!("protein: {:?}", protein);
                        let client = Arc::clone(&client);
                        let tx = tx.clone();
                        let sem_clone = Arc::clone(&semaphore);

                        tokio::spawn(async move {
                            //let (client, _connection) = get_db().await;
                            let _permit = sem_clone.acquire().await.expect("Failed to acquire permit");
                            process_protein(&client, protein).await.unwrap();
                            tx.send(()).await.expect("channel can be waited");
                        });
                        //process_protein(&client, protein).await.unwrap();

                        i += 1;
                        let iteration_duration = iteration_start_time.elapsed();
                        let total_elapsed = start_time.elapsed();
                        msg = format!(
                            "Iteration: {}, Iteration Time: {:.2?}, Total Time: {:.2?}",
                            i,
                            iteration_duration,
                            total_elapsed
                        );
                        bar.inc(1);
                        bar.set_message(msg);
                    } else if let Some((_parent_name, _parent_attrs, parent_children)) = stack.last_mut() {
                        //println!("building parents children");
                        parent_children.push(XmlEvent::StartElement {
                            name: xml::name::OwnedName { local_name: name.clone(), namespace: None, prefix: None },
                            attributes: attrs.into_iter().map(|(name, value)| xml::attribute::OwnedAttribute {
                                name: xml::name::OwnedName { local_name: name, namespace: None, prefix: None },
                                value
                            }).collect(),
                            namespace: xml::namespace::Namespace::empty()
                        });
                        parent_children.extend(children);
                        parent_children.push(XmlEvent::EndElement {
                            name: xml::name::OwnedName { local_name: name, namespace: None, prefix: None }
                        });
                    }
                }
            }
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
            _ => {}
        }
    }

    // Make sure all the async db tasks finish up
    while let Some(_) = rx.recv().await {}

    bar.finish_with_message("completed");
 
    Ok(())
}
