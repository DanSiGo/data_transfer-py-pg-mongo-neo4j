import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase

def connect_postgres(host, database, user, password):
    try:
        conn = psycopg2.connect(
            host = host,
            port = "5430",
            database = database,
            user = user,
            password = password
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error conecting to POSTGRES: {e}")
        return None
    
def connect_mongo(host, database, collection):
    try:
        client = MongoClient(host)
        db = client[database]
        coll = db[collection]
        return coll
    except Exception as e:
        print(f"Error connecting to MONGO: {e}")
        return None
    
def neo4j_connection (uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver
    
def get_material_id(file_value):
    with neo4j_driver.session() as session:
        result = session.run("MATCH (n:Material{File: $file_value}) RETURN n.id", file_value=file_value)
        material_id = result.single().get("n.id")
        return material_id
    
def get_course_id(file_value):
    with neo4j_driver.session() as session:
        chapter = session.run("MATCH (lo:LearningObjective {id: $file_value}) RETURN lo.chapter", file_value=file_value)
        chapter_result = chapter.single()
        chapter_id = chapter_result.get("lo.chapter")
        course = session.run("MATCH (ch:Chapter {id: $chapter_id}) RETURN ch.course", chapter_id=chapter_id)
        course_id = course.single().get("ch.course")
        return course_id

def transfer_data(postgres_conn, mongo_ClassMaterial, table):

    try:
        mongo_ClassMaterial.delete_many({})
        cur = postgres_conn.cursor()
        cur.execute(f"SELECT learning_object_id, id, transcript_id, mimetype FROM {table} WHERE transcript_id IS NOT NULL LIMIT 10")
        rows = cur.fetchall()

        for row in rows:
            transcript_doc = mongo_VideoLesson.find_one({"uuid": row[2]}) 
            transcript = transcript_doc["transcript"]
            material_id = get_material_id(row[1])
            course_id = get_course_id(row[0])

            doc = {
                "id" : row[2],
                "CourseId" : course_id,
                "ObjectiveId" : row[0],
                "MaterialId" : material_id,
                "Transcript" : transcript,
                "MaterialType" : row[3],
                "IsSuccessful" : True,
            }
            
            mongo_ClassMaterial.insert_one(doc) 

        print(f"Data transfered!")
    except Exception as e:
        print(f"Transfer error: {e}")

if __name__ == "__main__":
    postgres_conn = connect_postgres(
        host="localhost",
        database="homero",
        user="postgres",
        password="postgres"
    )

    mongo_ClassMaterial = connect_mongo(
        host="localhost",
        database="homero",
        collection="ClassMaterialPy"
    )

    mongo_VideoLesson = connect_mongo(
        host="localhost",
        database="homero",
        collection="VideoLesson"
    )

    neo4j_driver = neo4j_connection(
        uri = "bolt://localhost:7687",
        user = "neo4j",
        password = "1234qwer"
    )


    transfer_data(
        postgres_conn,
        mongo_ClassMaterial,
        table="media",
    )