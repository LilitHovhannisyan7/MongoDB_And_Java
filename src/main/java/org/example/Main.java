package org.example;

import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.util.ArrayList;
import java.util.List;
import static com.mongodb.client.model.Aggregates.group;




public class Main
{
    private MongoCollection<Document> collection;

    public void createStudent(String name, int age, String studentId, List<String> enrolledCourses) {
        Document student = new Document();
        student.put("name", name);
        student.put("age", age);
        student.put("studentId", studentId);
        student.put("enrolledCourses", enrolledCourses);
        collection.insertOne(student);
        System.out.println("Student added successfully.");
    }
    public void updateStudent(String studentId, List<String> newEnrolledCourses)
    {
        Document query = new Document("studentId", studentId);
        Document update = new Document("$set", new Document("enrolledCourses", newEnrolledCourses));
        collection.updateOne(query, update);
        System.out.println("Student details updated successfully.");
    }

    public void findStudentsByCourse(String courseId) {
        Document query = new Document("enrolledCourses", courseId);
        FindIterable<Document> result = collection.find(query);
        for(Document d : result)
        {
            System.out.println(d.toJson());
        }
    }
    public void deleteStudent(String studentId) {
        Document query = new Document("studentId", studentId);
        collection.deleteOne(query);
    }


    public void createIndexOnStudentId()
    {
        IndexOptions indexOptions = new IndexOptions().unique(true);
        collection.createIndex(new Document("studentId", 1), indexOptions); // 1 for ascending order
    }


    public void findStudentsCountByCourse() {
        List<Bson> pipeline = List.of(
                group("$enrolledCourses", Accumulators.sum("count", 1))
        );
        collection.aggregate(pipeline);
    }


    public void findAverageAgeByCourse() {
        List<Bson> pipeline = List.of(
                group("$enrolledCourses", Accumulators.avg("averageAge", "$age"))
        );

        collection.aggregate(pipeline);
    }


    public void addCourse(String courseId, String courseName, String department) {
        Document doc = new Document("courseId", courseId)
                .append("courseName", courseName)
                .append("department", department);
        collection.insertOne(doc);
    }



    public List<String> getCoursesForStudent(String studentId) {
        List<String> enrolledCourses = new ArrayList<>();

        Document studentDocument = collection.find(new Document("studentId", studentId)).first();
        if (studentDocument != null) {
            List<String> courseIds = studentDocument.getList("enrolledCourses", String.class);
            for (String courseId : courseIds) {
                Document courseDocument = collection.find(new Document("courseId", courseId)).first();
                if (courseDocument != null) {
                    enrolledCourses.add(courseDocument.getString("courseName"));
                }
            }
        }
        return enrolledCourses;
    }
    public static void main(String[] args)
    {
        try(MongoClient client = MongoClients.create("mongodb://localhost:27017"))
        {
            MongoDatabase database = client.getDatabase("SchoolDB");
            database.createCollection("Students");
            database.createCollection("Courses");
        }
        catch(Exception e)
        {
            System.out.println("Invalid");
        }


    }
}