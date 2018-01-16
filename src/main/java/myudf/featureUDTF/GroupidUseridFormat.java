package myudf.featureUDTF;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string->string,string"})
public class GroupidUseridFormat extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        String tagstr = (String) args[0];
        String uidstr = (String) args[1];
        if(tagstr !=null & uidstr!=null) {
            tagstr = tagstr.trim();
            uidstr = uidstr.trim();
            uidstr = uidstr.replace("[", "");
            uidstr = uidstr.replace("]", "");
            for (String u : uidstr.split(",")) {
                forward(u, tagstr);
            }
        }
    }

    @Override
    public void close() throws UDFException {

    }

}