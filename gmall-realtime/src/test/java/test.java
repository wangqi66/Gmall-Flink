import java.util.HashSet;

/**
 * @author wang
 * @create 2021-09-30 16:11
 */
public class test {

    public static void main(String[] args) {
        HashSet set = new HashSet<String>();
        set.add("a");
        set.add("b");
        set.add("c");
        set.add("d");

        HashSet<String> a = new HashSet<>();
        a.add("2");
        a.add("b");
        a.add("e");
        a.add("d");

        HashSet<HashSet> sets = new HashSet<>();
        sets.add(set);
        sets.add(a);


        System.out.println(sets.size());


    }


}
