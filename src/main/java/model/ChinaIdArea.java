package model;

/**
 * Created by Administrator on 2017/8/29 0029.
 */
public class ChinaIdArea {
    private String province;
    private String city;
    private String area;

    public ChinaIdArea(String province, String city, String area) {
        this.province = province;
        this.city = city;
        this.area = area;
    }

    public String getProvince() {
        return province;
    }

    public String getCity() {
        return city;
    }

    public String getArea() {
        return area;
    }
}
