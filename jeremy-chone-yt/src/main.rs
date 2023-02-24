// Rust Collections by Alexis Beingessner - He created the Rust BTree collection :)

#![allow(unused)]
use anyhow::{anyhow, Result};
// https://cglab.ca/~abeinges/blah/rust-btree-case/
use std::collections::BTreeMap;
use std::error::Error;
use surrealdb::sql::{thing, Datetime, Object, Thing, Value};
use surrealdb::{Datastore, Response, Session};


type DB = (Datastore, Session);
#[tokio::main]
async fn main() -> Result<()> {
   
// TO make this code tighter and pragmatic we will remove the hardwired code. 
//    let ds = Datastore::new("memory").await?;
//    let ses = Session::for_db("my_ns", "my_db");
    
// Type is an alias for existing types, Structs are newly created types
    
    let db: &DB = &(Datastore::new("memory").await?, Session::for_db("my_ns", "my_db"));
    let (ds, ses) = db;

// --- Create w/ ID set manually

//    let sql = "CREATE task:1 SET title = 'Task 01', priority = 10";
//    let ress = ds.execute(sql, &ses, None, false).await?;
//    let sql = "CREATE task:2 SET title = 'Task 02', priority = 5";
//    let ress = ds.execute(sql, &ses, None, false).await?;

/*   Output: Note the ID 
[Response { sql: None, time: 472.833µs, result: Ok(Array(Array([Object(Object({"id": Thing(Thing { tb: "task", id: Number(1) }), "priority": Number(Int(10)), "title": Strand(Strand("Task 01"))})), Object(Object({"id": Thing(Thing { tb: "task", id: Number(2) }), "priority": Number(Int(5)), "title": Strand(Strand("Task 02"))}))]))) }]
*/

// --- Create w/ ID set automatically by SurrealDB

// let sql = "CREATE task SET title = 'Task 01', priority = 10";
// let ress = ds.execute(sql, ses, None, false).await?;
// let sql = "CREATE task SET title = 'Task 02', priority = 5";
// let ress = ds.execute(sql, ses, None, false).await?;

/*   Output: Note the ID is randomized, will change each time you run it
[Response { sql: None, time: 477.291µs, result: Ok(Array(Array([Object(Object({"id": Thing(Thing { tb: "task", id: String("91l7x4ceqtityewlt0ht") }), "priority": Number(Int(10)), "title": Strand(Strand("Task 01"))})), Object(Object({"id": Thing(Thing { tb: "task", id: String("h7n2wijjwecab6jh1uwa") }), "priority": Number(Int(5)), "title": Strand(Strand("Task 02"))}))]))) }]
*/

// --- Pragmatic Create tasks
let t1 = create_task(db, "Task 01", 10).await?;
let t2 = create_task(db, "Task 02", 7).await?;

println!("{t1}, {t2}");
/*
Output:

[Response { sql: None, time: 451.709µs, result: Ok(Array(Array([Object(Object({"id": Thing(Thing { tb: "task", id: String("7333tvxt6g7r2gvkog5e") }), "priority": Number(Int(10)), "title": Strand(Strand("Task 01"))})), Object(Object({"id": Thing(Thing { tb: "task", id: String("osj63rqw92ku9pna2xib") }), "priority": Number(Int(7)), "title": Strand(Strand("Task 02"))}))]))) }]
 */

// --- Merge

let sql = "UPDATE $th MERGE $data RETURN id";
let data: BTreeMap<String, Value> = [
    ("title".into(), "Task 02 UPDATED".into()),
    ("done".into(), true.into()),
]
.into();

let vars: BTreeMap<String, Value> = [
    ("th".into(), thing(&t2)?.into()),
    ("data".into(), data.into()),

]
.into();
ds.execute(sql, ses, Some(vars), true).await?;
 /*
 Output:
 task:fus0lf2necavwiy8oozc, task:i6849kh9mnny8ixnmiki
record { id: task:fus0lf2necavwiy8oozc, priority: 10, title: "Task 01" } 
record { done: true, id: task:i6849kh9mnny8ixnmiki, priority: 7, title: "Task 02 UPDATED" } 
  */

  // --- Delete
  let sql = "DELETE $th";
  let vars: BTreeMap<String, Value> = [
    ("th".into(),
    thing(&t1)?.into())
  ]
  .into();
ds.execute(sql, ses, Some(vars), true).await?;
 /*
 Output
 task:qu539u6r272k9vw3zk7t, task:51klhcp0b7e6otfsyhy4
record { done: true, id: task:51klhcp0b7e6otfsyhy4, priority: 7, title: "Task 02 UPDATED" } 
  */

// --- Select
let sql = "SELECT * FROM task";
let ress = ds.execute(sql, &ses, None, false).await?;
// println!("{ress:?}");
for object in into_iter_objects(ress)? {
    println!("record {} ", object?);
     /* Output: 
    record { id: task:3qx1df4qwopi3q8n0cza, priority: 10, title: "Task 01" } 
    record { id: task:bxzy08y0cntrfkmx43gc, priority: 7, title: "Task 02" }
    */
    // println!("record {:?} ", object?.get("id"));

    /* Output: 
    record Some(Thing(Thing { tb: "task", id: String("2t50im12r1hnu0klhj8w") })) 
    record Some(Thing(Thing { tb: "task", id: String("o9xwjfy1gafn2k8qs6fx") }))
    */
}

   Ok(())
}

// async fn create_task((ds, ses): &DB, title: &str, priority: i32) -> Result<()> - Make this Pragmatic!
async fn create_task((ds, ses): &DB, title: &str, priority: i32) -> Result<(String)>
{
    let sql = "CREATE task CONTENT $data";

    let data: BTreeMap<String, Value> = [
        ("title".into(), title.into()),
        ("priority".into(), priority.into())
    ].into();

    let vars: BTreeMap<String, Value> = [
        ("data".into(), data.into())
    ].into();

    let ress = ds.execute(sql, ses, Some(vars), false).await?;
    into_iter_objects(ress)?
        .next()
        .transpose()?
        .and_then(|obj| obj.get("id")
        .map(|id| id.to_string()))
        .ok_or_else(|| anyhow!("No id returned."))



}

// Returns  Result<impl Iterator<Item = Result<Object>>>
// As a trick we can just use a void Response to get compile and then add the types later....
fn into_iter_objects(ress: Vec<Response>) -> Result<impl Iterator<Item = Result<Object>>> {
    let res = ress
        .into_iter()
        .next()
        .map(|rp| rp.result).transpose(); // This makes a Result of Options into Option of Results

    match res {
        Ok(Some(Value::Array(arr))) => {
            let it = arr
                .into_iter()
                .map(|v| match v {
                    Value::Object(object) => Ok(object),
                    _ => Err(anyhow!("A record was not an Object")),
                });
            Ok(it)
        }
                    _ => Err(anyhow!("No records found.")),
    }

   
}

