from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "neo"))

with driver.session() as session, \
        open("movies.emb", "w") as movies_file, \
        open("movies_labels.txt", "w") as movies_labels_file, \
        open("people.emb", "w") as people_file, \
        open("people_labels.txt", "w") as people_labels_file:
    result = session.run("""\
    CALL algo.deepgl.stream(null, null, {pruningLambda: 0.5})
    YIELD nodeId, embedding
    MATCH (n) WHERE id(n) = nodeId
    RETURN id(n) AS nodeId, coalesce(n.title, n.name) AS value, labels(n)[0] AS label, embedding
    ORDER BY label, nodeId
    """)

    for row in result:
        if row["label"] == "Movie":
            movies_file.write("{0}\n".format(" ".join([str(e) for e in row["embedding"]])))
            movies_labels_file.write("\"{0}\"\n".format(row["value"]))

        if row["label"] == "Person":
            people_file.write("{0}\n".format(" ".join([str(e) for e in row["embedding"]])))
            people_labels_file.write("\"{0}\"\n".format(row["value"]))