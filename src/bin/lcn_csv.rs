/*
Changing approach, will do a few passes getting the records
of interest one at a time
1. protein
2. match
3. lcn
4. ipr

Generate a csv straight to disc, not loading into the db yet.
Last we'll load into the db from the separate csvs

*/
use std::fs::File;
use std::time::{Instant, Duration};
use std::io::BufReader;
use xml::reader::{EventReader, XmlEvent};
use csv::Writer;
use std::collections::HashMap;

use indicatif::{ProgressBar, ProgressStyle};

fn main() -> Result<(), Box<dyn std::error::Error>> {

    let file = File::open("/mnt/md0/data/interpro/match_complete.xml").unwrap();
    let file = BufReader::new(file);
    let parser = EventReader::new(file);

    let mut i = 0;

    let bar = ProgressBar::new_spinner();
    bar.enable_steady_tick(Duration::new(0, 100000000));
    let style = ProgressStyle::default_spinner()
        .template("{spinner} {wide_msg}")?;
    bar.set_style(style);

    let start_time = Instant::now();
    let mut msg = String::new();

    let mut wtr = Writer::from_path("/mnt/md0/data/interpro/lcn_records.csv")?;

    // Write the header row
    wtr.write_record(&[
        "protein_id",
        "match_id",
        "id",
        "start",
        "end",
        "fragments",
        "score",
    ])?;

    let protein_id: String = String::new();
    let match_id: String = String::new();

    for e in parser {
        let iteration_start_time = Instant::now();
        match e {
            Ok(XmlEvent::StartElement { name, attributes, .. }) => {
                if name.local_name.as_str() == "protein" {
                    let attrs: Vec<(String, String)> = attributes.into_iter().map(|a| (a.name.local_name, a.value)).collect();
                    protein_id = attrs[0].1.clone();
                else if name.local_name.as_str() == "match" {
                    let attrs: Vec<(String, String)> = attributes.into_iter().map(|a| (a.name.local_name, a.value)).collect();
                    match_id = attrs[0].1.clone();
                else if name.local_name.as_str() == "lcn" {
                    // Attrs (key, value): id, name, length, crc64
                    let attrs: HashMap<String, String> = attributes.into_iter().map(|a| (a.name.local_name, a.value)).collect();

                    // write a record to the CSV file
                    wtr.write_record(&[
                        protein_id.clone(),
                        match_id.clone(),
                        attrs['id'].clone(),
                        attrs['start'].clone(),
                        attrs['end'].clone(),
                        attrs['fragments'].clone(),
                        attrs['score'].clone(),
                    ])?;

                    if (i % 100 == 0) {
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
                    i += 1;
                }
            }
            Ok(XmlEvent::Characters(_content)) => {
                unimplemented!("characters"); // File shouldn't contain text in the xml elements
            }
            Ok(XmlEvent::EndElement { name: _end_name }) => {
                // Skip for protein
            }
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
            _ => {}
        }
    }

    // Write any buffered records to disk
    wtr.flush()?;

    bar.finish_with_message("completed");

    Ok(())
}