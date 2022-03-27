import java.io.*;
import java.util.*;

class Main {

public static void main(String args[])
{
	String matrix_filename = args[0];
	String vector_filename = args[1];

	// Read the vector in a list of floats
	ArrayList<Float> vector = new ArrayList<Float>();

	try {
		File vectorFile = new File(vector_filename);
		Scanner vectorReader = new Scanner(vectorFile);

		while (vectorReader.hasNextLine()) {
			vector.add(Float.valueOf(vectorReader.nextLine()));
		}

		vectorReader.close();

		System.out.print("Vector of size: ");
		System.out.println(vector.size());
	} catch (FileNotFoundException e) {
		System.out.print("File not found: ");
		System.out.println(vector_filename);
	}

	// Read the matrix and produce output elements as the matrix is read (so that it does not have to be stored in memory, and the code is simpler)
	try {
		File matrixFile = new File(matrix_filename);
		Scanner matrixReader = new Scanner(matrixFile);

		while (matrixReader.hasNextLine()) {
			StringTokenizer tok = new StringTokenizer(matrixReader.nextLine());

			// Sum of the element-wise products of this line (in tok) and "vector"
			Float sum = new Float(0.0);
			int i = 0;

			while (tok.hasMoreTokens()) {
				sum += vector.get(i) * Float.valueOf(tok.nextToken());
				i += 1;
			}

			System.out.println(sum);
		}

		matrixReader.close();
	} catch (FileNotFoundException e) {
		System.out.print("Matrix file not found: ");
		System.out.println(matrix_filename);
	}
}

}
