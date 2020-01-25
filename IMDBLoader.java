
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;


public class IMDBLoader {

    static  String folderToAssignment;
    static String dbURL;
    static String pwd;
    static  String user;
    static  String pathToIMDBData;
    static  String maxMem;


    private Connection con;
    private Statement statement;



    public IMDBLoader() throws ClassNotFoundException, SQLException {



        Class.forName("com.mysql.jdbc.Driver");



    }



        public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

             folderToAssignment = "IMDB//src//main//java";
             dbURL = args[0];
             user = args[1];
             pwd = args[2];
             pathToIMDBData = args[3];



            IMDBLoader im = new IMDBLoader();
            //im.createDB();
            //im.createTables();
            //im.getRating();
            //im.createpersonTable();
            im.createrelatTables();





        }

    private void createDB() throws SQLException {
        Statement stm=null;
        try {

            Class.forName("com.mysql.jdbc.Driver");

            con = DriverManager.getConnection("jdbc:mysql://localhost:3306",user,pwd);


             stm = con.createStatement();
            stm.execute("CREATE DATABASE IMDBLoader");



        }catch (Exception e){


        }finally {
            stm.close();

        }


        }

    private void createrelatTables() throws SQLException {



        String principal =pathToIMDBData+"//"+"title.principals.tsv.gz";





        String regex = "(?<=^..)(.*)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher_patt_per = null;
        Matcher matcher_patt_movie = null;



        String actedIn_create_sql = "CREATE TABLE IF NOT EXISTS ActedIn (\n"
                + "    personId int ,\n"
                + "    movieId int NOT NULL , \n"
                + "PRIMARY KEY (personId,movieId) ,\n"
                +  "FOREIGN KEY (personId) REFERENCES person(id), FOREIGN KEY (movieId) REFERENCES movie(id) \n"
                + ");";

        String composedBy_create_sql = "CREATE TABLE IF NOT EXISTS ComposedBy (\n"
                + "    personId int ,\n"
                + "    movieId int NOT NULL , \n"
                + "PRIMARY KEY(personId,movieId), \n"
                +  "FOREIGN KEY (personId) REFERENCES person(id) ,  FOREIGN KEY (movieId) REFERENCES movie(id) \n"
                + ");";

        String directedBy_create_sql = "CREATE TABLE IF NOT EXISTS DirectedBy (\n"
                + "    personId int ,\n"
                + "    movieId int NOT NULL ,\n"
                + "PRIMARY KEY(personId,movieId) ,\n"
                +  "FOREIGN KEY (personId) REFERENCES person(id) ,  FOREIGN KEY (movieId) REFERENCES movie(id) \n"
                + ");";

        String editedBy_create_sql = "CREATE TABLE IF NOT EXISTS EditedBy (\n"
                + "    personId int ,\n"
                + "    movieId int NOT NULL ,\n"
                + "PRIMARY KEY(personId,movieId) ,\n"
                +  "FOREIGN KEY (personId) REFERENCES person(id) ,  FOREIGN KEY (movieId) REFERENCES movie(id) \n"
                + ");";

        String producedBy_create_sql = "CREATE TABLE IF NOT EXISTS ProducedBy (\n"
                + "    personId int ,\n"
                + "    movieId int NOT NULL , \n"
                + "PRIMARY KEY(personId,movieId) ,\n"
                +  "FOREIGN KEY (personId) REFERENCES person(id) ,  FOREIGN KEY (movieId) REFERENCES movie(id) \n"

                + ");";

        String writtenBy_create_sql = "CREATE TABLE IF NOT EXISTS WrittenBy (\n"
                + "    personId int ,\n"
                + "    movieId int NOT NULL , \n"
                + "PRIMARY KEY(personId,movieId), \n"
                +  "FOREIGN KEY (personId) REFERENCES person(id) ,  FOREIGN KEY (movieId) REFERENCES movie(id) \n"
                + ");";


        try{

            Class.forName("com.mysql.jdbc.Driver");

            con = DriverManager.getConnection(
                    dbURL, user, pwd
            );

            Statement statement = con.createStatement();

            statement.execute("USE  IMDBLoader");

            InputStream gzip = new GZIPInputStream(new FileInputStream(principal));

            BufferedReader br = new BufferedReader(new InputStreamReader(gzip));

            String row;

            Statement stmt = con.createStatement();

            stmt.execute(actedIn_create_sql);
            stmt.execute(composedBy_create_sql);
            stmt.execute(directedBy_create_sql);
            stmt.execute(editedBy_create_sql);
            stmt.execute(producedBy_create_sql);
            stmt.execute(writtenBy_create_sql);


            PreparedStatement psa = con.prepareStatement("INSERT IGNORE INTO ActedIn(personId,movieId) \n "
                    +" SELECT person.id , movie.id from person,movie where person.id = ? and movie.id = ?   ");


            PreparedStatement psc = con.prepareStatement("INSERT IGNORE INTO ComposedBy(personId,movieId) \n "
                    +" SELECT person.id , movie.id from person,movie where person.id = ? and movie.id = ?   ");


            PreparedStatement psd = con.prepareStatement("INSERT IGNORE INTO DirectedBy(personId,movieId) \n "
                    +" SELECT person.id , movie.id from person,movie where person.id = ? and movie.id = ?   ");

            PreparedStatement pse = con.prepareStatement("INSERT IGNORE INTO EditedBy(personId,movieId) \n "
                    +" SELECT person.id , movie.id from person,movie where person.id = ? and movie.id = ?   ");

            PreparedStatement psp = con.prepareStatement("INSERT IGNORE INTO ProducedBy(personId,movieId) \n "
                    +" SELECT person.id , movie.id from person,movie where person.id = ? and movie.id = ?   ");

            PreparedStatement psw = con.prepareStatement("INSERT IGNORE INTO WrittenBy(personId,movieId) \n "
                    +" SELECT person.id , movie.id from person,movie where person.id = ? and movie.id = ?   ");


            HashMap<String, Integer> pers_count = new HashMap<String, Integer>();
            HashMap<String, PreparedStatement> pers_work = new HashMap<String, PreparedStatement>();

            pers_count.put("actor",0);
            pers_count.put("composer",0);
            pers_count.put("director",0);
            pers_count.put("editor",0);
            pers_count.put("producer",0);
            pers_count.put("writer",0);


            pers_work.put("actor",psa);
            pers_work.put("composer",psc);
            pers_work.put("director",psd);
            pers_work.put("editor",pse);
            pers_work.put("producer",psp);
            pers_work.put("writer",psw);

            con.setAutoCommit(false);

            int count_row = 0;
            while((row = br.readLine())!=null){

                String result_per;
                String result_mov;

                if(count_row++==0)
                    continue;

                String [] data = row.split("\t");

                String prof = data[3];

                matcher_patt_per = pattern.matcher(data[2]);
                matcher_patt_movie = pattern.matcher(data[0]);



                if(prof.equals("actor") || prof.equals("actress") || prof.equals("self")){
                    prof = "actor";
                }

                if(pers_count.containsKey(prof) == false )
                    continue;

                else {

                    //System.out.println(count_row++);

                    if(matcher_patt_per.find() && matcher_patt_movie.find()) {
                         result_per = matcher_patt_per.group(1);
                        pers_work.get(prof).setInt(1, Integer.parseInt(result_per));

                         result_mov = matcher_patt_movie.group(1);
                        pers_work.get(prof).setInt(2,  Integer.parseInt(result_mov));

                        pers_count.put(prof, pers_count.get(prof) + 1);
                        pers_work.get(prof).addBatch();

                    }
                    else{
                        continue;
                    }

                    if (pers_count.get(prof) % 50000 == 0) {
                        System.out.println("Here");
                        pers_work.get(prof).executeBatch();
                    }
                }

            }

            psa.executeBatch();
            psc.executeBatch();
            psd.executeBatch();
            pse.executeBatch();
            psp.executeBatch();
            psw.executeBatch();


            con.commit();


        }
        catch (Exception e){


            System.out.println(e);
        }

    }

    private void createpersonTable() {


        String person_create_sql = "CREATE TABLE IF NOT EXISTS person (\n"
                + "    id int PRIMARY KEY,\n"
                + "    name text NOT NULL,\n"
                + "    birthYear int ,\n"
                + "    deathYear     int"
                + ");";

        String ratings = pathToIMDBData+"//"+"name.basics.tsv.gz";
        String regex = "(?<=^..)(.*)";
        Pattern pattern = Pattern.compile(regex);

        try {
            int counter=0;

            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(
                    dbURL, user, pwd
            );

            Statement statement = con.createStatement();

            statement.execute("USE  IMDBLoader");

            InputStream gzip = new GZIPInputStream(new FileInputStream(ratings));

            BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
            String row;

            Statement stmt = con.createStatement();

            stmt.execute(person_create_sql);


            PreparedStatement ps = con.prepareStatement("INSERT INTO person(id,name,birthYear,deathYear) \n "
                    + " VALUES (?, ? , ? , ?)");

            int last_val = 0;
            int count = 0;


            con.setAutoCommit(false); // default true

            while ((row = br.readLine()) != null) {
                String result="";

                Matcher matcher_patt;

                //  System.out.println(count);
                if (count == 0) {
                    count = count + 1;
                    continue;
                }


                String[] data = row.split("\t");




                    matcher_patt = pattern.matcher(data[0]);
                    if (matcher_patt.find()) {
                         result = matcher_patt.group(1);


                        ps.setInt(1, Integer.parseInt(result));

                    }

                    ps.setNString(2, data[1]);
                    if (data[2].equals("\\N"))
                        ps.setInt(3, Integer.parseInt("-1"));

                    else
                        ps.setInt(3, Integer.parseInt(data[2]));
                    if (data[3].equals("\\N"))
                        ps.setInt(4, Integer.parseInt("-1"));

                    else
                        ps.setInt(4, Integer.parseInt(data[3]));


                    ps.addBatch();


                    if (++count % 50000 == 0) {
                        System.out.println("Here");
                        ps.executeBatch();
                    }


                }
                System.out.println(counter);
                ps.executeBatch();
                con.commit();


            } catch(IOException ex){
                ex.printStackTrace();
            } catch(SQLException ex){
                ex.printStackTrace();
            } catch(ClassNotFoundException e){
                e.printStackTrace();
            }


        }



        private void getRating() throws SQLException, IOException {

        String ratings = pathToIMDBData+"//"+"title.ratings.tsv.gz";
            String regex = "(?<=^..)(.*)";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher_patt = null;


            try {


                con = DriverManager.getConnection(
                        dbURL, user, pwd
                );


            Statement statement = con.createStatement();

            statement.execute("USE  IMDBLoader");

            InputStream gzip = new GZIPInputStream(new FileInputStream(ratings));

            BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
            String row;

            PreparedStatement ps = con.prepareStatement("UPDATE movie set rating = ?,numberOfVotes = ? \n "
                    + " where id=?");

            int count = 0;
            con.setAutoCommit(false); // default true

            while ((row = br.readLine()) != null) {
                String result;


                if (count == 0) {
                    count = count + 1;
                    continue;
                }


                String[] data = row.split("\t");

                matcher_patt = pattern.matcher(data[0]);

                if(matcher_patt.find()) {
                    result = matcher_patt.group(1);


                    ps.setInt(3, Integer.parseInt(result));
                    ps.setFloat(1, Float.parseFloat(data[1]));
                    ps.setFloat(2, Float.parseFloat(data[2]));
                }

                else{
                    continue;
                }




                ps.addBatch();

                if (++count % 50000 == 0) {
                    System.out.println("Here");
                    ps.executeBatch();
                }


            }

            ps.executeBatch();
            con.commit();


        }catch (Exception e){

            System.out.println(e);

        }finally {
            con.close();
        }
    }


    private void createTables() throws SQLException, IOException {

        HashMap<String,Integer> genre = new HashMap<String,Integer>();

        String movie = pathToIMDBData+"//"+"title.basics.tsv.gz";


        String regex = "(?<=^..)(.*)";
        Pattern pattern = Pattern.compile(regex);

        String movie_create_sql = "CREATE TABLE IF NOT EXISTS movie (\n"
                + "    id int PRIMARY KEY,\n"
                + "    title text NOT NULL,\n"
                + "    releaseYear int ,\n"
                + "    runTime     int   ,\n"
                + "    rating      float ,\n"
                + "    numberOfVotes  float \n"
                + ");";

        String genre_create_sql = "CREATE TABLE IF NOT EXISTS genre (\n"
                + "    id int PRIMARY KEY,\n"
                + "    name text NOT NULL\n"
                + ");";

        String has_genre_create_sql = "CREATE TABLE IF NOT EXISTS HasGenre (\n"
                + "    movie_id int ,\n"
                + "    genre_id int NOT NULL, PRIMARY KEY (movie_id,genre_id) ,\n"
                + "    FOREIGN KEY (movie_id) REFERENCES movie(id) , FOREIGN KEY (genre_id) REFERENCES genre(id)\n "
                + ");";



        /* Crete Tables*/
        try {

            con = DriverManager.getConnection(
                    dbURL, user, pwd
            );


            Statement statement = con.createStatement();

            statement.execute("USE  IMDBLoader");

            statement.execute(movie_create_sql);

            statement.execute(genre_create_sql);

            statement.execute(has_genre_create_sql);



            InputStream gzip = new GZIPInputStream(new FileInputStream(movie));

        BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
        String row ;

        PreparedStatement ps = con.prepareStatement("INSERT INTO movie(id,title,releaseYear,runTime,rating,numberOfVotes) \n "
                             +" VALUES (?, ? , ? , ? , ? ,?)" );

            PreparedStatement   ps1 = con.prepareStatement("INSERT INTO genre(id,name) \n "
                    +" VALUES (?, ?)" );

            PreparedStatement   ps2 = con.prepareStatement("INSERT INTO HasGenre(movie_id,genre_id) \n "
                    +" VALUES (?, ?)" );

            int count = 0;
            int count_genre = 0;
            int genre_id = 1;
            con.setAutoCommit(false); // default true

            while ((row = br.readLine()) != null) {
                Matcher matcher_patt;
                String result;

                if (count == 0) {
                    count = count + 1;
                    continue;
                }


                String[] data = row.split("\t");


                matcher_patt = pattern.matcher(data[0]);

                if(matcher_patt.find()) {
                        result = matcher_patt.group(1);


                        ps.setInt(1, Integer.parseInt(result));
                    }
                    else{
                        continue;
                    }

                if (data[1].equals("short") || data[1].equals("movie") || data[1].equals("tvMovie") || data[1].equals("tvShort")) {

                    ps.setNString(2, data[2]);

                    if (data[5].equals("\\N"))
                        ps.setNString(3, null);
                    else
                        ps.setNString(3, data[5]);

                    if (data[7].equals("\\N"))
                        ps.setNString(4, null);
                    else
                        ps.setNString(4, data[7]);




                    String [] genres = data[8].split(",");

                    for(String genre_name :genres){

                        if(genre.containsKey(genre_name) == false){

                            genre.put(genre_name,genre_id);

                            ps1.setInt(1,genre_id);
                            ps1.setString(2,genre_name);
                            genre_id++;
                            ps1.execute();

                        }


                        ps2.setInt(1,Integer.parseInt(result));

                        ps2.setInt(2,genre.get(genre_name));

                        ps2.addBatch();





                    }


                    ps.setNString(5, "0.0");
                    ps.setNString(6, "0.0");



                    ps.addBatch();



                    if (++count_genre % 50000 == 0) {
                        ps.executeBatch();
                        System.out.println("Here");
                        ps2.executeBatch();
                    }


                }
                else {
                    continue;
                }
            }

            ps.executeBatch(); // insert remaining records
            ps2.executeBatch();
            con.commit();




        }catch (Exception e){
            System.out.println(e);
        }
        finally {
            con.close();
        }


    }


}
