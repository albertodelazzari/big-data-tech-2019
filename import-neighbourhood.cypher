LOAD CSV WITH HEADERS FROM "file:///Users/albertodelazzari/Downloads/listings.csv" AS row FIELDTERMINATOR ','
MERGE (n:Neighborhood {neighborhood_id: coalesce(row.neighbourhood_cleansed, "Milan")})
SET n.name = row.neighbourhood
MERGE (c:City {name: "Milan"})
MERGE (l)-[:IN_NEIGHBORHOOD]->(n)
MERGE (n)-[:LOCATED_IN]->(c)
MERGE (s:Province {name: "Lombardy"})
MERGE (c)-[:IN_STATE]->(s)
MERGE (country:Country {code: coalesce(row.country_code, "IT")})
SET country.name = row.country
MERGE (s)-[:IN_COUNTRY]->(country)
