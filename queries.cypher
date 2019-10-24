// Customized content based recommendation for a certain user, Gabriele (93165866)
MATCH (u:User {user_id: "93165866"})-[:WROTE]->(r:Review)-[:REVIEWS]->(l:Listing)-[:HAS]->(a:Amenity)
MATCH (a)<-[:HAS]-(rec:Listing)
RETURN rec.name, COUNT(DISTINCT a) AS score ORDER BY score DESC LIMIT 10


// Customized content based (neighborhoods) recommendation for a certain user, Gabriele
MATCH (u:User {user_id: "93165866"})-[:WROTE]->(r:Review)-[:REVIEWS]->(l:Listing)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
WITH u, l, COLLECT(DISTINCT n) AS neighborhoods
MATCH (l:Listing)-[:HAS]->(a:Amenity)
MATCH (rec)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
WITH rec, n, neighborhoods, COUNT(DISTINCT a) AS score WHERE n IN neighborhoods
RETURN rec.name, score ORDER BY score DESC LIMIT 10

// Collaborative filtering for a certain user, Gabriele, based on his reviews
MATCH (u:User {user_id: "93165866"})-[:WROTE]->(r:Review)-[:REVIEWS]->(l:Listing)
MATCH (l)<-[:REVIEWS]-(:Review)<-[:WROTE]-(other:User)-[:WROTE]-(:Review)-[:REVIEWS]->(rec:Listing)
RETURN rec.name, COUNT(*) AS score ORDER BY score DESC LIMIT 10

// Co-occurence network for listings that are often reviewed by the same users
CALL apoc.periodic.iterate('
MATCH (l1:Listing)
WHERE size((l1)<-[:REVIEWS]->()) > 10
RETURN l1
','
MATCH (l1)<-[:REVIEWS]-(r1:Review)
MATCH (r1)<-[:WROTE]-(u:User)
MATCH (u)-[:WROTE]->(r2:Review)
MATCH (r2)-[:REVIEWS]->(l2:Listing)
WHERE id(l1) < id(l2)
WITH l1, l2, COUNT(*) AS weight where weight > 5
MERGE (l1)-[cr:CO_OCCURENT_REVIEWS]-(l2)
ON CREATE SET cr.weight = weight
',{batchSize: 1})


CALL apoc.periodic.iterate(
"MATCH (p1:User)-[]->(:Review)-[:REVIEWS]->(l1:Listing) WHERE size((p1)-[:WROTE]->()) > 5 RETURN p1, l1",
"
MATCH (p2:User)-[:WROTE]->(r2)-[:REVIEWS]->(l2:Listing), (p2:User)-[:WROTE]->()-[:REVIEWS]->(l1)
WITH p1, p2, l1, l2
WHERE id(p1) < id(p2) AND size((p2)-[:WROTE]->()) > 10
WITH p1,p2,count(*) as coop, collect(id(l1)) as s1, collect(id(l2)) as s2 where coop > 10
WITH p1,p2, apoc.algo.cosineSimilarity(s1,s2) as cosineSimilarity WHERE cosineSimilarity > 0
MERGE (p1)-[s:SIMILAR_REVIEWS]-(p2) SET s.weight = cosineSimilarity"
, {batchSize:100, parallel:false,iterateList:true});
