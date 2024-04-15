const express = require("express");
const { Pool } = require("pg");
const cors = require("cors");

const app = express();
app.use(cors());
//Create a new Pool instance to manage connections to PostgreSQL
const pool = new Pool({
  user: "radhika",
  host: "localhost",
  database: "7dbs",
  port: 5432, // Default PostgreSQL port
  max: 1000, //setting maximum number of clients in the pool
  idleTimeoutMillis: 120000, //Close clients that are idle after 120 seconds
  connectionTimeoutMillis: 5000, //Setting timeout for client connections. This controls how long they wait for an available connection from the pool
});

//PostgreSQL Connection Test
pool.query("SELECT NOW()", (err, result) => {
  if (err) {
    console.error("Error executing query", err);
  } else {
    console.log("Connected to PostgreSQL database at", result.rows[0].now);
  }
});

//caching results for performance improvement
//Assumption: database data is static
//Cache invalidation can be implemented in scenario where the database gets updated.
//Cache needs to be invalidated before database is updated.
//If cache is not invalidated before updating the database, if invalidation fails, it can lead to inconsistency between cached data and current data in database.
//Here, only caching at server side is implemented for the query results (with and without author_name)

let topAuthorsCache = null; //caching result of top performing authors
let authorNameCache = null; // caching author_name
let authorSpecificCachedData = null; // caching result for author_name

//EndPoint to fetch data of top 10 performing authors, optionally filtered by author_name
app.get("/topAuthors", async (req, res) => {
  try {
    // fetch value of author's name
    const authorName = req.query.author_name;

    if (!authorName && topAuthorsCache) {
      console.log("sending cached data for top 10 performing authors.");
      return res.status(200).json(topAuthorsCache);
    }
    if (authorName && authorName == authorNameCache) {
      console.log("sending data from cache for the requested author.");
      return res.status(200).json(authorSpecificCachedData);
    }

    //fetching the name as well as email of the author for displaying on UI
    let topRevenueAuthorsQuery = `SELECT a.name, a.email, SUM(si.item_price * si.quantity) AS total_revenue 
    FROM authors a JOIN books b on a.id = b.author_id 
    JOIN sale_items si on b.id = si.book_id`;

    const queryParams = [];

    if (authorName) {
      topRevenueAuthorsQuery += ` WHERE LOWER(a.name) = LOWER($1)`;
      queryParams.push(authorName);
    }

    topRevenueAuthorsQuery += ` GROUP BY a.name, a.email 
    ORDER BY total_revenue DESC
    LIMIT 10`;

    //execute the query
    const { rows } = await pool.query(topRevenueAuthorsQuery, queryParams);

    if (rows.length === 0) {
      //No results found, return 404 Not Found
      res.status(404).json({
        error: "Invalid author name. Please provide a valid author name.",
      });
    } else {
      //caching for recent requested author name
      if (authorName) {
        authorNameCache = authorName;
        authorSpecificCachedData = rows;
      } else {
        //caching the result rows when author name is not provided
        topAuthorsCache = rows;
      }
      //send the fetched data
      res.status(200).json(rows);
    }
  } catch (error) {
    console.error("Error executing query", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

//Handle for unmatched routes and methods
app.use((req, res, next) => {
  res
    .status(404)
    .json({ error: "Resource not found, URL endpoint or path is incorrect." });
});

//handle invalid request methods
app.use((err, req, res, next) => {
  res
    .status(405)
    .json({ error: "Method not allowed, verify URL path and requirements" });
});

app.listen(3000, () => {
  console.log("Server is running on port 3000");
});
