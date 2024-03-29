# Distil

This is a work-in-progress proof-of-concept of a SQL query engine for JSON data written in Rust.
This was built as an experiment as part of my journey learning Rust. As part of this it is not written optimally as I'm exploring the concepts within the Rust language. The end goal is to build this into a streaming data analytics tool.

The project is not near MVP stage and minimal functionality has been implemented. An example of the output is shown below. This performs a SELECT query against a JSON object and delivers the following results.

SQL Input Query
```SQL
SELECT
	payload.version AS version, 
	payload.meta.id AS id, version + 5, 
	payload.data.payload AS "abc" 
FROM "/topic" 
WHERE (version-1) = 0
```
JSON Input Data
```JSON
{
  "data": {
    "payload": [
      1,
      2,
      3,
      4,
      5
    ]
  },
  "meta": {
    "id": 2
  },
  "version": 1
}
```
Query results
```
	"version" -> Number(1)
	"id" -> Number(2)
	"2" -> Number(6)
	"abc" -> Array [Number(1), Number(2), Number(3), Number(4), Number(5)]
	WHERE: Bool(true)
```