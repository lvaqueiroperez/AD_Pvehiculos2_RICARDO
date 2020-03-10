//VARIANTE DEL PROYECTO PVEHICULOS2 CON ENUNCIADO EN PAPEL
package pvehiculos2;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import org.bson.Document;

public class Pvehiculos2 {

    //VARIABLES ORACLE
    private static int id;
    private static String dni;
    private static String codveh;
    private static java.math.BigDecimal tasas;
    //VARIABLES MONGO
    private static String nomveh;
    private static double prezoorixe;
    private static double anomatricula;
    private static String nomec;
    private static double ncompras;
    private static double pf;

    public static void ejercicio() throws SQLException {

        //CONEXION A ORACLE
        Connection conn;
        String driver = "jdbc:oracle:thin:";
        String host = "localhost.localdomain"; // tambien puede ser una ip como "192.168.1.14"
        String porto = "1521";
        String sid = "orcl";
        String usuario = "hr";
        String password = "hr";
        String url = driver + usuario + "/" + password + "@" + host + ":" + porto + ":" + sid;

        conn = DriverManager.getConnection(url);

        PreparedStatement ps = conn.prepareStatement("select * from vendas");

        ResultSet rs = ps.executeQuery();

        while (rs.next()) {

            id = rs.getInt("id");
            System.out.println("ID: " + id);
            dni = rs.getString("dni");
            System.out.println("DNI: " + dni);

            //AHORA OBTENEMOS LOS DATOS DE LA VARIABLE OBJETO (RS.GETOBJECT RECIBE LA POSICIÓN DEL OBJETO EN LA TABLA)
            java.sql.Struct objSQL = (java.sql.Struct) rs.getObject(3);

            Object[] campos = objSQL.getAttributes();

            codveh = (String) campos[0];
            tasas = (java.math.BigDecimal) campos[1];

            System.out.println("CODVEH: " + codveh);
            System.out.println("TASAS: " + tasas);

            //AHORA OBTENEMOS LAS VARIABLES DE MONGO
            //CONEXIÓN MONGO
            MongoClient mongoClient = new MongoClient("localhost", 27017);

            MongoDatabase database = mongoClient.getDatabase("basevehiculos");

            //COLECCIÓN VEHICULOS
            MongoCollection<Document> collection = database.getCollection("vehiculos");

            //USAMOS ITERABLE PORQUE VAMOS A DEVOLVER MÁS DE UN RESULTADO
            //NO HACE FALTA EL "INCLUDE" AL FINAL PORQUE NECESITAMOS TODOS LOS RESULTADOS, NO UNO O VARIOS EN CONCRETO
            FindIterable<Document> buscar = collection.find(eq("_id", codveh));

            for (Document z : buscar) {

                nomveh = z.getString("nomveh");
                System.out.println("NOMVEH: " + nomveh);
                prezoorixe = z.getDouble("prezoorixe");
                System.out.println("PREZOORIXE: " + prezoorixe);
                anomatricula = z.getDouble("anomatricula");
                System.out.println("ANOMATRICULA: " + anomatricula);

            }

            //COLECCIÓN CLIENTES:
            MongoCollection<Document> collection2 = database.getCollection("clientes");

            FindIterable<Document> buscar2 = collection2.find(eq("_id", dni));

            for (Document z : buscar2) {

                nomec = z.getString("nomec");
                System.out.println("NOMEC: " + nomec);
                ncompras = z.getDouble("ncompras");
                System.out.println("NCOMPRAS: " + ncompras);
                System.out.println("*********************************************");
            }

            //CALCULAMOS PF:
            if (ncompras != 0) {

                pf = prezoorixe - ((2019 - anomatricula) * 500) - 500 + tasas.doubleValue();

            } else {

                pf = prezoorixe - ((2019 - anomatricula) * 500) - 0 + tasas.doubleValue();

            }

            //HORA QUE TENEMOS TODOS LOS DATOS, LOS METEMOS COMO OBJETOS VENFIN EN OBJECTDB
            //LO METEMOS TODO EN OBJECTDB, LA BASE DE DATOS ES CREADA SI NO EXISTE !!!
            //OJO!!! HACER UNA TRANSACCIÓN POR CADA OPERACIÓN Y CERRAR AL FINAL !!!
            EntityManagerFactory emf = Persistence.createEntityManagerFactory("$objectdb/db/finalveh.odb");

            EntityManager em = emf.createEntityManager();

            em.getTransaction().begin();

            Venfin obj = new Venfin((double) id, dni, nomec, nomveh, pf);
            em.persist(obj);

            em.getTransaction().commit();

            //CERRAMOS AQUÍ O MÁS ADELANTE???
            em.close();

            mongoClient.close();

        }

        conn.close();

    }

    public static void main(String[] args) throws SQLException {

        Pvehiculos2.ejercicio();

    }

}
