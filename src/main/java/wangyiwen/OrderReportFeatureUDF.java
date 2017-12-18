package wangyiwen;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

import java.util.*;

@Resolve({"string,string,string,string,string,string,string,string->string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string," +
        "string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string," +
        "string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string"})

public class OrderReportFeatureUDF extends UDTF {
    public static int count = 948;

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        String report = (String) args[6];
        System.out.println(report);
        ReadContext rct = JsonPath.parse(report);
        Object[] feature = new Object[count+1];
        for(int i = 1; i<=count; i++) {
            try {
                feature[i] = rct.read("$." + i + ".value") + "";
                System.out.print(i + ": " + feature[i] + "  ");
            }catch(Exception e) {
                feature[i] = "";
            }
            System.out.println();
        }
        forward(args[0], args[1],args[2],args[3],args[4],args[5],
        feature[1],feature[2],feature[7],feature[11],feature[14],feature[15],feature[22],feature[26],feature[27],feature[32],feature[35],feature[36],feature[39],feature[40],feature[41],feature[49],feature[51],feature[264],feature[566],feature[660],feature[661],feature[662],feature[663],feature[664],feature[665],feature[666],feature[667],feature[668],feature[669],feature[670],feature[671],feature[672],feature[673],feature[674],feature[675],feature[676],feature[677],feature[678],feature[679],feature[680],feature[681],feature[682],feature[683],feature[684],feature[685],feature[686],feature[687],feature[688],feature[689],feature[690],feature[691],feature[692],feature[693],feature[694],feature[748],feature[749],feature[750],feature[751],feature[752],feature[753],feature[754],feature[755],feature[756],feature[757],feature[758],feature[759],feature[761],feature[762],feature[763],feature[764],feature[765],feature[766],feature[767],feature[768],
        feature[769],feature[772],feature[773],feature[774],feature[775],feature[776],feature[777],feature[778],feature[779],feature[780],feature[781],feature[782],feature[783],feature[785],feature[786],feature[787],feature[788],feature[789],feature[790],feature[791],feature[792],feature[793],feature[794],feature[795],feature[796],feature[797],feature[798],feature[799],feature[800],feature[801],feature[802],feature[803],feature[804],feature[805],feature[806],feature[807],feature[808],feature[809],feature[810],feature[811],feature[812],feature[813],feature[814],feature[815],feature[816],feature[817],feature[818],feature[819],feature[820],feature[821],feature[822],feature[823],feature[824],feature[825],feature[826],feature[827],feature[828],feature[830],feature[831],feature[832],feature[833],feature[834],feature[835],feature[837],feature[838],feature[839],feature[840],feature[841],feature[842],feature[843],feature[844],feature[845],feature[846],feature[16],feature[17],feature[18],
                feature[23],feature[24],feature[62],feature[63],feature[64],feature[65],feature[66],feature[67],feature[68],
                feature[700],feature[701],feature[703],feature[704],feature[706],feature[707],feature[72],feature[73],feature[75],
                feature[847],feature[848],feature[850],feature[851],feature[853],feature[854],feature[855],feature[856],feature[857],
                feature[858],feature[859],feature[860],feature[861],feature[862],feature[863],feature[864],feature[865],feature[339],feature[873],feature[874],feature[875],feature[261],
                feature[165],feature[204],feature[887],feature[888],feature[889],feature[890],feature[891],feature[892],feature[893],feature[894],feature[895],feature[896],feature[897],feature[898],feature[899],feature[900],feature[901],feature[902],feature[903],feature[904],feature[905],feature[906],
                feature[737],feature[739], feature[738], feature[248], feature[265], feature[881], feature[884], feature[885], feature[44], feature[52], feature[546], feature[926],
                feature[925], feature[927], feature[928], feature[929], feature[930], feature[931], feature[932], feature[933], feature[934], feature[935], feature[936], feature[937],
                feature[938], feature[939], feature[940], feature[941], feature[942], feature[943], feature[944], feature[945], feature[946], feature[947], feature[948],
                args[7]);
    }

    @Override
    public void close() throws UDFException {

    }
}
