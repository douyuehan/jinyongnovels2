package com.neuedu;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;

import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//        String str = "西红柿炖牛腩怎么做？";
//        DicLibrary.insert(DicLibrary.DEFAULT, "西红柿炖牛腩", "n", 1000);//设置自定义分词  n代表名词 1000代表默认出现的频率
        String str = "柯镇恶大踏步走到厅中，铁杖在方砖上一落，当的一声，悠悠不绝，嘶哑着嗓子道：“梅超风，你瞧不见我，我也瞧不见你。那日荒山夜战，你丈夫死于非命，我们张五弟却也给你们害死了，你知道么？”梅超风道：“哦，只剩下六怪了。”柯镇恶道：“我们答应了马钰马道长，不再向你寻仇为难，今日却是你来找我们。好罢，天地虽宽，咱们却总是有缘，处处碰头。老天爷不让六怪与你梅超风在世上并生，进招罢。”梅超风冷笑道：“你们六人齐上。”朱聪等早站在大哥身旁相护，防梅超风忽施毒手，这时各亮兵刃。郭靖忙道：“仍是让弟子先挡一阵。”陆乘风听梅超风与六怪双方叫阵，心下好生为难，有意要替两下解怨，只恨自己威不足以服众、艺不足以惊人，听到郭靖这句话，心念忽动，说道：“各位且慢动手，听小弟一言。梅师姊与六侠虽有宿嫌，但双方均已有人不幸下世，依兄弟愚见，今日只赌胜负，点到为止，不可伤人，六侠以六敌一，虽是向来使然，总觉不公，就请梅师姊对这位郭老弟教几招如何？”梅超风冷笑道：“我岂能跟无名小辈动手？”郭靖叫道：“你丈夫是我亲手杀的，与我师父何干？”梅超风悲怒交迸，喝道：“正是，先杀你这小贼。”听声辨形，左手疾探，五指猛往郭靖天灵盖插下。郭靖急跃避开，叫道：“梅前辈，晚辈当年无知，误伤了陈老前辈，一人作事一人当，你只管问我。今日你要杀要剐，我决不逃走。若是日后你再找我六位师父啰唣，那怎么说？”他料想今日与梅超风对敌，多半要死在她爪底，却要解去师父们的危难。梅超风道：“你真的有种不逃？”郭靖道：“不逃。”梅超风道：“好！我和江南六怪之事，也是一笔勾销。好小子，跟我走罢！”黄蓉叫道：“梅师姊，他是好汉子，你却叫江湖上英雄笑歪了嘴。”梅超风怒道：“怎么？”黄蓉道：“他是江南六侠的嫡传弟子。六侠的武功近年来已大非昔比，他们要取你性命真是易如反掌，今日饶了你，还给你面子，你却不知好歹，尚在口出大言。”梅超风怒道：“呸！我要他们饶？六怪，你们武功大进了？那就来试试？”黄蓉道：“他们何必亲自和你动手？单是他们的弟子一人，你就未必能胜。”梅超风大叫：“三招之内我杀不了他，我当场撞死在这里。”他在赵王府曾与郭靖动过手，深知他武功底细，却不知数月之间，郭靖得九指神丐传授绝艺，功夫已然大进。";

        str = str.replace("道","");
        DicLibrary.insert(DicLibrary.DEFAULT, "柯镇恶");
        DicLibrary.insert(DicLibrary.DEFAULT, "梅超风");

        Result result = DicAnalysis.parse(str);
        List<Term> termList=result.getTerms();
        for(Term term:termList){
            System.out.println(term.getName()+":"+term.getNatureStr());
        }

    }
}


