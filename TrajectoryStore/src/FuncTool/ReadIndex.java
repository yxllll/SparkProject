package FuncTool;

import edu.ecnu.idse.TrajStore.core.CellInfo;
import edu.ecnu.idse.TrajStore.core.SpatialTemporalSite;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by zzg on 15-12-23.
 */
public class ReadIndex {
    public static  void main(String[] args) throws IOException{
        String indexPath = "/home/zzg/IdeaProjects/worksapce/indexes/kd.txt";
        BufferedReader br = new BufferedReader(new FileReader(indexPath));
        String line ;

        int count =0;
        while((line = br.readLine()) != null){
            count++;
        }
        br.close();

        String [] tokens=null;
        br = new BufferedReader(new FileReader(indexPath));
        CellInfo[] infos = new CellInfo[count];
        int i = 0;
        int id;
        double blo,bla,elo,ela;
        String [] first = null;
        String [] second = null;
        while((line = br.readLine()) != null){
            tokens =line.split("\t");
            id = Integer.parseInt(tokens[0]);
            first = tokens[1].split(" ");
            second = tokens[2].split(" ");
            blo = Double.parseDouble(first[0]);
            bla = Double.parseDouble(first[1]);
            elo = Double.parseDouble(second[0]);
            ela = Double.parseDouble(second[1]);
            infos[i++] = new CellInfo(id,blo,bla,elo,ela);
            System.out.println(id+"\t"+blo+" "+bla+"\t"+elo+" "+ela);
        }
        Configuration conf  = new Configuration();

        SpatialTemporalSite.writeSpatialIndex(conf,infos);

    }
}
