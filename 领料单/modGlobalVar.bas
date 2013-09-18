Attribute VB_Name = "modGlobalVar"
''''''''''''''''''''''''''''''''''''''
'全局变量模块(用于存放全局变量)
''''''''''''''''''''''''''''''''''''''

'牌号集合
Public pfphlist As Collection
'配方表集合
Public formulaTableList As Collection
'投料表集合
Public supplyTableList As Collection

'日投料批次关键坐标
Public ws1_r1 As Integer, ws1_c1 As Integer
Public ws1_r2 As Integer, ws1_c2 As Integer
Public ws1_r3 As Integer, ws1_c3 As Integer
Public ws1_r4 As Integer, ws1_c4 As Integer

'领料单关键坐标
Public ws2_r1 As Integer, ws2_c1 As Integer
Public ws2_r2 As Integer, ws2_c2 As Integer

'配置页面关键坐标
Public ws3_r1 As Integer, ws3_c1 As Integer
Public ws3_r2 As Integer, ws3_c2 As Integer

'配方单页面关键坐标
Public wsp_r1 As Integer, wsp_c1 As Integer
Public wsp_r2 As Integer, wsp_c2 As Integer

'全局变量初始化
Public Function globalVarInit()
    '日投料批次关键坐标
    modGlobalVar.ws1_c1 = 1
    modGlobalVar.ws1_r1 = 5
    
    modGlobalVar.ws1_c2 = 2
    modGlobalVar.ws1_r2 = 5
    
    modGlobalVar.ws1_c3 = 4
    modGlobalVar.ws1_r3 = 5
    
    modGlobalVar.ws1_c4 = 4
    modGlobalVar.ws1_r4 = 3
    
    '领料单关键坐标
    modGlobalVar.ws2_c1 = 2
    modGlobalVar.ws2_r1 = 2
    
    modGlobalVar.ws2_c2 = 6
    modGlobalVar.ws2_r2 = 2
    
    '配置页面关键坐标
    modGlobalVar.ws3_c1 = 2
    modGlobalVar.ws3_r1 = 4
    
    modGlobalVar.ws3_c2 = 6
    modGlobalVar.ws3_r2 = 4
    
    '配方单页面关键坐标
    modGlobalVar.wsp_c1 = 2
    modGlobalVar.wsp_r1 = 2
    
    modGlobalVar.wsp_c2 = 2
    modGlobalVar.wsp_r2 = 4
    
End Function
