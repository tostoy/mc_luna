package wangyiwen;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

import java.util.*;

@Resolve({"string,string,bigint,string,bigint,string,bigint,string->string,string,bigint,string,bigint,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,bigint,string"})
public class CreditReportUDF extends UDTF {

    public static int count = 1125;

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        String report = (String) args[5];
        System.out.println(report);
        ReadContext rct = JsonPath.parse(report);
        Object[] feature = new Object[count+1];
        for(int i = 1; i<=count; i++) {
            try {
//                // put the parsed json into a sorted hash map
//                LinkedHashMap<String, String> map = rct.read("$." + i);
//                // parse through the sorted hash map
//                Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
//                while (iterator.hasNext()) {
//                    Map.Entry entry = iterator.next();
//                    if(entry.getKey().equals("value")) {
//                        // use the forward function to pass out the output
//                        forward(entry.getValue());
//                    }
//                }
                feature[i] = rct.read("$." + i + ".value") + "";
                System.out.print(i + ": " + feature[i] + "  ");
            }catch(Exception e) {
                feature[i] = "";
            }
            System.out.println();
        }
        forward(args[0],args[1],args[2],args[3],args[4],
                feature[1], feature[2], feature[3], feature[4], feature[5], feature[6], feature[7], feature[8], feature[9], feature[10], feature[11], feature[12], feature[13], feature[14], feature[15], feature[16], feature[17], feature[18], feature[19], feature[20], feature[21], feature[22], feature[23], feature[24], feature[25], feature[26], feature[27], feature[28], feature[29], feature[30], feature[31], feature[32], feature[33], feature[34], feature[35], feature[36], feature[37], feature[38], feature[39],
                feature[40], feature[41], feature[42], feature[43], feature[44], feature[45], feature[46], feature[47], feature[48], feature[49], feature[50], feature[51], feature[52], feature[53], feature[54], feature[55], feature[56], feature[57], feature[58], feature[59],
                feature[60], feature[61], feature[62], feature[63], feature[64], feature[65], feature[66], feature[67], feature[68], feature[69], feature[70], feature[71], feature[72], feature[73], feature[74], feature[75], feature[76], feature[77], feature[78], feature[79],
                feature[80], feature[81], feature[82], feature[83], feature[84], feature[85], feature[86], feature[87], feature[88], feature[89], feature[90], feature[91], feature[92], feature[93], feature[94], feature[95], feature[96], feature[97], feature[98], feature[99],
                feature[100], feature[101], feature[102], feature[103], feature[104], feature[105], feature[106], feature[107], feature[108], feature[109], feature[110], feature[111], feature[112], feature[113], feature[114], feature[115], feature[116], feature[117], feature[118], feature[119],
                feature[120], feature[121], feature[122], feature[123], feature[124], feature[125], feature[126], feature[127], feature[128], feature[129], feature[130], feature[131], feature[132], feature[133], feature[134], feature[135], feature[136], feature[137], feature[138], feature[139],
                feature[140], feature[141], feature[142], feature[143], feature[144], feature[145], feature[146], feature[147], feature[148], feature[149], feature[150], feature[151], feature[152], feature[153], feature[154], feature[155], feature[156], feature[157], feature[158], feature[159],
                feature[160], feature[161], feature[162], feature[163], feature[164], feature[165], feature[166], feature[167], feature[168], feature[169], feature[170], feature[171], feature[172], feature[173], feature[174], feature[175], feature[176], feature[177], feature[178], feature[179],
                feature[180], feature[181], feature[182], feature[183], feature[184], feature[185], feature[186], feature[187], feature[188], feature[189], feature[190], feature[191], feature[192], feature[193], feature[194], feature[195], feature[196], feature[197], feature[198], feature[199],
                feature[200], feature[201], feature[202], feature[203], feature[204], feature[205], feature[206], feature[207], feature[208], feature[209], feature[210], feature[211], feature[212], feature[213], feature[214], feature[215], feature[216], feature[217], feature[218], feature[219],
                feature[220], feature[221], feature[222], feature[223], feature[224], feature[225], feature[226], feature[227], feature[228], feature[229], feature[230], feature[231], feature[232], feature[233], feature[234], feature[235], feature[236], feature[237], feature[238], feature[239],
                feature[240], feature[241], feature[242], feature[243], feature[244], feature[245], feature[246], feature[247], feature[248], feature[249], feature[250], feature[251], feature[252], feature[253], feature[254], feature[255], feature[256], feature[257], feature[258], feature[259],
                feature[260], feature[261], feature[262], feature[263], feature[264], feature[265], feature[266], feature[267], feature[268], feature[269], feature[270], feature[271], feature[272], feature[273], feature[274], feature[275], feature[276], feature[277], feature[278], feature[279],
                feature[280], feature[281], feature[282], feature[283], feature[284], feature[285], feature[286], feature[287], feature[288], feature[289], feature[290], feature[291], feature[292], feature[293], feature[294], feature[295], feature[296], feature[297], feature[298], feature[299],
                feature[300], feature[301], feature[302], feature[303], feature[304], feature[305], feature[306], feature[307], feature[308], feature[309], feature[310], feature[311], feature[312], feature[313], feature[314], feature[315], feature[316], feature[317], feature[318], feature[319],
                feature[320], feature[321], feature[322], feature[323], feature[324], feature[325], feature[326], feature[327], feature[328], feature[329], feature[330], feature[331], feature[332], feature[333], feature[334], feature[335], feature[336], feature[337], feature[338], feature[339],
                feature[340], feature[341], feature[342], feature[343], feature[344], feature[345], feature[346], feature[347], feature[348], feature[349], feature[350], feature[351], feature[352], feature[353], feature[354], feature[355], feature[356], feature[357], feature[358], feature[359],
                feature[360], feature[361], feature[362], feature[363], feature[364], feature[365], feature[366], feature[367], feature[368], feature[369], feature[370], feature[371], feature[372], feature[373], feature[374], feature[375], feature[376], feature[377], feature[378], feature[379],
                feature[380], feature[381], feature[382], feature[383], feature[384], feature[385], feature[386], feature[387], feature[388], feature[389], feature[390], feature[391], feature[392], feature[393], feature[394], feature[395], feature[396], feature[397], feature[398], feature[399],
                feature[400], feature[401], feature[402], feature[403], feature[404], feature[405], feature[406], feature[407], feature[408], feature[409], feature[410], feature[411], feature[412], feature[413], feature[414], feature[415], feature[416], feature[417], feature[418], feature[419],
                feature[420], feature[421], feature[422], feature[423], feature[424], feature[425], feature[426], feature[427], feature[428], feature[429], feature[430], feature[431], feature[432], feature[433], feature[434], feature[435], feature[436], feature[437], feature[438], feature[439],
                feature[440], feature[441], feature[442], feature[443], feature[444], feature[445], feature[446], feature[447], feature[448], feature[449], feature[450], feature[451], feature[452], feature[453], feature[454], feature[455], feature[456], feature[457], feature[458], feature[459],
                feature[460], feature[461], feature[462], feature[463], feature[464], feature[465], feature[466], feature[467], feature[468], feature[469], feature[470], feature[471], feature[472], feature[473], feature[474], feature[475], feature[476], feature[477], feature[478], feature[479],
                feature[480], feature[481], feature[482], feature[483], feature[484], feature[485], feature[486], feature[487], feature[488], feature[489], feature[490], feature[491], feature[492], feature[493], feature[494], feature[495], feature[496], feature[497], feature[498], feature[499],
                feature[500], feature[501], feature[502], feature[503], feature[504], feature[505], feature[506], feature[507], feature[508], feature[509], feature[510], feature[511], feature[512], feature[513], feature[514], feature[515], feature[516], feature[517], feature[518], feature[519],
                feature[520], feature[521], feature[522], feature[523], feature[524], feature[525], feature[526], feature[527], feature[528], feature[529], feature[530], feature[531], feature[532], feature[533], feature[534], feature[535], feature[536], feature[537], feature[538], feature[539],
                feature[540], feature[541], feature[542], feature[543], feature[544], feature[545], feature[546], feature[547], feature[548], feature[549], feature[550], feature[551], feature[552], feature[553], feature[554], feature[555], feature[556], feature[557], feature[558], feature[559],
                feature[560], feature[561], feature[562], feature[563], feature[564], feature[565], feature[566], feature[567], feature[568], feature[569], feature[570], feature[571], feature[572], feature[573], feature[574], feature[575], feature[576], feature[577], feature[578], feature[579],
                feature[580], feature[581], feature[582], feature[583], feature[584], feature[585], feature[586], feature[587], feature[588], feature[589], feature[590], feature[591], feature[592], feature[593], feature[594], feature[595], feature[596], feature[597], feature[598], feature[599],
                feature[600], feature[601], feature[602], feature[603], feature[604], feature[605], feature[606], feature[607], feature[608], feature[609], feature[610], feature[611], feature[612], feature[613], feature[614], feature[615], feature[616], feature[617], feature[618], feature[619],
                feature[620], feature[621], feature[622], feature[623], feature[624], feature[625], feature[626], feature[627], feature[628], feature[629], feature[630], feature[631], feature[632], feature[633], feature[634], feature[635], feature[636], feature[637], feature[638], feature[639],
                feature[640], feature[641], feature[642], feature[643], feature[644], feature[645], feature[646], feature[647], feature[648], feature[649], feature[650], feature[651], feature[652], feature[653], feature[654], feature[655], feature[656], feature[657], feature[658], feature[659],
                feature[660], feature[661], feature[662], feature[663], feature[664], feature[665], feature[666], feature[667], feature[668], feature[669], feature[670], feature[671], feature[672], feature[673], feature[674], feature[675], feature[676], feature[677], feature[678], feature[679],
                feature[680], feature[681], feature[682], feature[683], feature[684], feature[685], feature[686], feature[687], feature[688], feature[689], feature[690], feature[691], feature[692], feature[693], feature[694], feature[695], feature[696], feature[697], feature[698], feature[699],
                feature[700], feature[701], feature[702], feature[703], feature[704], feature[705], feature[706], feature[707], feature[708], feature[709], feature[710], feature[711], feature[712], feature[713], feature[714], feature[715], feature[716], feature[717], feature[718], feature[719],
                feature[720], feature[721], feature[722], feature[723], feature[724], feature[725], feature[726], feature[727], feature[728], feature[729], feature[730], feature[731], feature[732], feature[733], feature[734], feature[735], feature[736], feature[737], feature[738], feature[739],
                feature[740], feature[741], feature[742], feature[743], feature[744], feature[745], feature[746], feature[747], feature[748], feature[749], feature[750], feature[751], feature[752], feature[753], feature[754], feature[755], feature[756], feature[757], feature[758], feature[759],
                feature[760], feature[761], feature[762], feature[763], feature[764], feature[765], feature[766], feature[767], feature[768], feature[769], feature[770], feature[771], feature[772], feature[773], feature[774], feature[775], feature[776], feature[777], feature[778], feature[779],
                feature[780], feature[781], feature[782], feature[783], feature[784], feature[785], feature[786], feature[787], feature[788], feature[789], feature[790], feature[791], feature[792], feature[793], feature[794], feature[795], feature[796], feature[797], feature[798], feature[799],
                feature[800], feature[801], feature[802], feature[803], feature[804], feature[805], feature[806], feature[807], feature[808], feature[809], feature[810], feature[811], feature[812], feature[813], feature[814], feature[815], feature[816], feature[817], feature[818], feature[819],
                feature[820], feature[821], feature[822], feature[823], feature[824], feature[825], feature[826], feature[827], feature[828], feature[829], feature[830], feature[831], feature[832], feature[833], feature[834], feature[835], feature[836], feature[837], feature[838], feature[839],
                feature[840], feature[841], feature[842], feature[843], feature[844], feature[845], feature[846], feature[847], feature[848], feature[849], feature[850], feature[851], feature[852], feature[853], feature[854], feature[855], feature[856], feature[857], feature[858], feature[859],
                feature[860], feature[861], feature[862], feature[863], feature[864], feature[865], feature[866], feature[867], feature[868], feature[869], feature[870], feature[871], feature[872], feature[873], feature[874], feature[875], feature[876], feature[877], feature[878], feature[879],
                feature[880], feature[881], feature[882], feature[883], feature[884], feature[885], feature[886], feature[887], feature[888], feature[889], feature[890], feature[891], feature[892], feature[893], feature[894], feature[895], feature[896], feature[897], feature[898], feature[899],
                feature[900], feature[901], feature[902], feature[903], feature[904], feature[905], feature[906], feature[907], feature[908], feature[909], feature[910], feature[911], feature[912], feature[913], feature[914], feature[915], feature[916], feature[917], feature[918], feature[919],
                feature[920], feature[921], feature[922], feature[923], feature[924], feature[925], feature[926], feature[927], feature[928], feature[929], feature[930], feature[931], feature[932], feature[933], feature[934], feature[935], feature[936], feature[937], feature[938], feature[939],
                feature[940], feature[941], feature[942], feature[943], feature[944], feature[945], feature[946], feature[947], feature[948], feature[949], feature[950], feature[951], feature[952], feature[953], feature[954], feature[955], feature[956], feature[957], feature[958], feature[959],
                feature[960], feature[961], feature[962], feature[963], feature[964], feature[965], feature[966], feature[967], feature[968], feature[969], feature[970], feature[971], feature[972], feature[973], feature[974], feature[975], feature[976], feature[977], feature[978], feature[979],
                feature[980], feature[981], feature[982], feature[983], feature[984], feature[985], feature[986], feature[987], feature[988], feature[989], feature[990], feature[991], feature[992], feature[993], feature[994], feature[995], feature[996], feature[997], feature[998], feature[999],
                feature[1000], feature[1001], feature[1002], feature[1003], feature[1004], feature[1005], feature[1006], feature[1007], feature[1008], feature[1009], feature[1010], feature[1011], feature[1012], feature[1013], feature[1014], feature[1015], feature[1016], feature[1017], feature[1018], feature[1019],
                feature[1020], feature[1021], feature[1022], feature[1023], feature[1024], feature[1025], feature[1026], feature[1027], feature[1028], feature[1029], feature[1030], feature[1031], feature[1032], feature[1033], feature[1034], feature[1035], feature[1036], feature[1037], feature[1038], feature[1039],
                feature[1040], feature[1041], feature[1042], feature[1043], feature[1044], feature[1045], feature[1046], feature[1047], feature[1048], feature[1049], feature[1050], feature[1051], feature[1052], feature[1053], feature[1054], feature[1055], feature[1056], feature[1057], feature[1058], feature[1059],
                feature[1060], feature[1061], feature[1062], feature[1063], feature[1064], feature[1065], feature[1066], feature[1067], feature[1068], feature[1069], feature[1070], feature[1071], feature[1072], feature[1073], feature[1074], feature[1075], feature[1076], feature[1077], feature[1078], feature[1079],
                feature[1080], feature[1081], feature[1082], feature[1083], feature[1084], feature[1085], feature[1086], feature[1087], feature[1088], feature[1089], feature[1090], feature[1091], feature[1092], feature[1093], feature[1094], feature[1095], feature[1096], feature[1097], feature[1098], feature[1099],
                feature[1100], feature[1125],  args[6],args[7]);
    }

    @Override
    public void close() throws UDFException {

    }
}
