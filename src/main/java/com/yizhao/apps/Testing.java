package com.yizhao.apps;

/**
 * Testing data:
        3077146869351|16491|2120|305402241992|2120|1522861|2017-02-09 10:25:38
        3077146869351|17647|roomguru|305402241992|2120|1522861|2017-02-09 10:25:38
        3077146869351|17398|hm|305402241992|2120|1522861|2017-02-09 10:25:38
        3077146867791|19816|2479|305494389375|2479|1682191|2017-02-09 10:25:39
 * Output:
        ckvraw|1486664738|305402241992|16491=2120&17647=roomguru&17398=hm|3077146869351|2120|null|1522861|null|null|null
        ckvraw|1486664739|305494389375|19816=2479|3077146867791|2479|null|1682191|null|null|null
 */
public class Testing {
    public static void main(String[] args){
        FastrackFileProcessor.execute("/Users/yzhao/Desktop/test.txt", "/Users/yzhao/Desktop/result.txt");
    }
}
