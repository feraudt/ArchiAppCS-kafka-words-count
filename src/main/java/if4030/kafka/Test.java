package if4030.kafka;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Test {

	public static void main(String[] args) {
		System.out.println( "hello" );
		try {
			BufferedReader reader = new BufferedReader( new FileReader( "/config/workspace/src/main/resources/Lexique383.tsv" ));
			String header = reader.readLine();
			System.out.println( header );
			Map<String,String> lex = new HashMap<>();
			String line;
			while ( ( line = reader.readLine() ) != null ) {
				String[] splittedLine = line.split("\t");
				lex.put(splittedLine[0], splittedLine[2]);
			}
			reader.close();
			System.out.println( "lex a:" );
			System.out.println( lex.get("a") );
			System.out.println( "lex fghjklm:" );
			System.out.println( lex.get("fghjklm") );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
	