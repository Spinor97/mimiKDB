use chrono::{DateTime, NaiveDate, NaiveTime, Utc};

pub fn combine_date_time(md_entry_date: &str, md_entry_time: &str) -> DateTime<Utc>{
    let date = NaiveDate::parse_from_str(md_entry_date, "%Y%m%d")
        .expect("Failed to parse MDEntryDate");
        
    // 2. Parse time (Format %H:%M:%S%.3f for 3 decimal millisecond precision)
    // Alternatively, %f matches any number of fractional digits
    let time = if md_entry_time.len() == 12 {
        NaiveTime::parse_from_str(md_entry_time, "%H:%M:%S%.3f")
        .expect("Failed to parse MDEntryTime")
    } else {
        NaiveTime::parse_from_str(md_entry_time, "%H:%M:%S%")
        .expect("Failed to parse MDEntryTime")
    };

    // 3. Combine into a NaiveDateTime
    let naive_datetime = date.and_time(time);

    // 4. Convert to UTC DateTime
    naive_datetime.and_utc()
}