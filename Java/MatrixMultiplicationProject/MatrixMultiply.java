/*
Group Number: 7
Group Members:
	1.Sriram Nalabolu(700740102)
	2.V P S Abhay Bharath Polisetty(700740446)
	3.Rohith Verma Kadari(700739895)
	4.Prasanth Sagar Talluru(700734537)
	5.Venkata Ramana Cherukuri(700742704)
*/

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class MatrixMultiply
{
    //Driver code
    public static void main(String args[]) throws Exception
    {
        //Checking if 3 arguments are passed from yarn command as input
        if(args.length!=3)
        {
            System.out.println("Usage: Matrix Multiplication <input path> <output path> <matrices size>");
            System.exit(-1);
        }

        String[] matrices = args[2].split("");

        //Checking if the matrices size are valid
        if(Integer.parseInt(matrices[2])!=Integer.parseInt(matrices[4]))
        {
            System.out.println("Error: Incorrect matrices, Matrices should be of size m*n,n*p");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Matrix Multiplication");

        //Setting row,column sizes of two matrices and passing to mappers and reducers
        job.getConfiguration().set("firstrow",String.valueOf(matrices[0]));
        job.getConfiguration().set("firstcolumn",String.valueOf(matrices[2]));
        job.getConfiguration().set("secondrow",String.valueOf(matrices[4]));
        job.getConfiguration().set("secondcolumn",String.valueOf(matrices[6]));

        job.setJarByClass(MatrixMultiply.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    //Mapper class
    public static class MatrixMapper extends Mapper<LongWritable,Text,Text,Text>
    {

        /*Map method taking Byte Offset as Key,Each line from input text file as Value and processing (Text,Text) as
         *output from mapper using Context. */
        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
        {
            String line = value.toString();

            //Each line from input is stored as string in an array seperated by ','.
            String[] words=line.split(",");

            String out1;
            String out2="";
            out1=words[0];
            out2+=words[1]+',';
            out2+=words[2]+',';
            out2+=words[3];

            //Processing the output by generating Matrix name as one output and Row,Column,Value as another output
            context.write(new Text(out1),new Text(out2));
        }
    }

    //Reducer class
    public static class MatrixReducer extends Reducer<Text,Text,Text,Text>
    {
    	int count=0;

    	String[] mat1;
    	String[] mat2;

    	String matrix="";

        String firstrow;
        String firstcolumn;
        String secondrow;
        String secondcolumn;

        //Setup method to store the values in reducer from additional input
        @Override
        public void setup(Context context)
        {
            Configuration conf = context.getConfiguration();
            firstrow=conf.get("firstrow");
            firstcolumn=conf.get("firstcolumn");
            secondrow=conf.get("secondrow");
            secondcolumn=conf.get("secondcolumn");
        }

        /*Reduce method taking Matrix name as Key,array of "Row,Column,Value" Text common to the Matrix name as Value and
         *processing (Text,Text) as Reducer output using Context. */
        @Override
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
        {
            String rowstr="";
            String colstr="";

            //Storing each Text from an array of input Texts into a String seperated by ";".
            for(Text value:values)
            {
                if(count==0)
                   rowstr+=value+";";
                if(count==1)
                   colstr+=value+";";
            }

            //Storing each String as Array of strings defining it as first matrix and adding matrix name to an empty string
            if(count==0)
            {
               mat1=rowstr.split(";");
               matrix=key.toString();
               count+=1;
            }
            //Storing each String as Array of strings defining it as second matrix and concatenating matrix name to matrix string
            else
            {
               mat2=colstr.split(";");
               matrix+=key.toString();

               //Invoking finalStr method
               String[] finalList = finalStr();
               for(int i=0;i<finalList.length;i++)
               {
                   context.write(new Text(matrix),new Text(finalList[i]));
               }
            }
        }

        //Method to return string of Resultant Matrix schema as "Row,Column,Value"
        public String[] finalStr()
        {
            String[] strrows = rowMat(Integer.parseInt(firstrow),mat1,Integer.parseInt(firstcolumn));
		    String[] strcols = colMat(Integer.parseInt(secondrow),mat2,Integer.parseInt(secondcolumn));

            String totstr="";

            for(int i=0;i<strrows.length;i++)
            {
                for(int j=0;j<strcols.length;j++)
                {
                    String result=matMul(strrows[i],strcols[j]);
                    totstr+=String.valueOf(i)+","+String.valueOf(j)+","+result+";";
                }
            }

            String finalstr[] = totstr.split(";");

            return finalstr;
        }

        //Method to return array of Row Strings from First Matrix
        public static String[] rowMat(int rowIndex,String[] mat,int columnIndex)
        {
            String[] finalString = new String[rowIndex];

            for(int i=0;i<rowIndex;i++)
            {
                String string1="";
                for(int j=0;j<columnIndex;j++)
                {
                    for(int k=0;k<mat.length;k++)
                    {
                        String[] list1 = mat[k].split(",");
                        if(Integer.parseInt(list1[0])==i && Integer.parseInt(list1[1])==j)
                        {
                            if(j==columnIndex-1)
                                string1+=list1[2];
                            else
                                string1+=list1[2]+",";
                        }
                    }
                }
                finalString[i]=string1;
            }
            return finalString;
        }

        //Method to return array of Column Strings from Second Matrix
        public static String[] colMat(int rowIndex,String[] mat,int columnIndex)
        {
            String[] finalString = new String[columnIndex];

            for(int i=0;i<columnIndex;i++)
            {
                String string1="";
                for(int j=0;j<rowIndex;j++)
                {
                    for(int k=0;k<mat.length;k++)
                    {
                        String[] list1 = mat[k].split(",");
                        if(Integer.parseInt(list1[1])==i && Integer.parseInt(list1[0])==j)
                        {
                            if(j==rowIndex-1)
                                string1+=list1[2];
                            else
                                string1+=list1[2]+",";
                        }
                    }
                }
                finalString[i]=string1;
            }
            return finalString;
        }

        //Method to multiply a Row from first matrix and a Column from second matrix and return the result as String
        public static String matMul(String strrows,String strcols)
        {
            String[] row = strrows.split(",");
            String[] col = strcols.split(",");
            int sum=0;
            for(int i=0;i<row.length;i++)
            {
                sum+=Integer.parseInt(row[i])*Integer.parseInt(col[i]);
            }

            return String.valueOf(sum);
        }
    }
}
