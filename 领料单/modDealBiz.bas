Attribute VB_Name = "modDealBiz"
''''''''''''''''''''''''''''
'业务处理模块
''''''''''''''''''''''''''''

'
Public Function dbiz_initProdTable()
    
    Dim phlist As Collection
    
    Set phlist = gdPfphList()
    
    dorg_initProdTable ThisWorkbook.Worksheets(1), phlist
    
    MsgBox "初始化完成"

End Function

'全牌号领料表计算
Public Function dbiz_computeSupplylistT(ByRef stl As Collection) As clsSupplyTable
    Dim st_t As New clsSupplyTable
    
    Dim st As clsSupplyTable
    Dim stt As clsSupplyTobacco
    
    For Each st In stl
        With st_t
            .llnf = st.llnf
            .llyf = st.llyf
            .llr = st.llr
            .pfph = "全牌号"
        End With
        
        For Each stt In st.supTobaccos
            st_t.addTobacco stt
        Next
    Next
    
    Set dbiz_computeSupplylistT = st_t
End Function

'领料表集合计算
Public Function dbiz_computeSupplyList(da As Integer, dt As Date)

    Dim pnums As Collection
    Dim pnum As clsFormulaProdNum
    Dim ft As clsFormulaTable
    Dim st_t As clsSupplyTable
    Dim sto As clsSupplyTable
    Dim stl As New Collection
    
    Set pnums = modGetData.gdGetFormulaProdNum(da)
    
    Set modGlobalVar.formulaTableList = modGetData.gdGetFormulaTable
    
    For Each pnum In pnums
    
        If (pnum.getProdNumofDay(da)) <> 0 Then
            Set ft = modGlobalVar.formulaTableList(pnum.pfph)
            Set sto = dbiz_supplyTable(da, pnum, ft)
            If Not (sto Is Nothing) Then
                stl.Add sto
            End If
        End If
    Next
    
    dorg_generateSupplyTableList stl, dt
    
    Set st_t = dbiz_computeSupplylistT(stl)
    
    dorg_generateSupplyTableListT st_t, dt
    'dorg_modifySuppliedNum stl
    
    MsgBox "计算领料单完成"

End Function

'单牌号领料表计算
Public Function dbiz_supplyTable(da As Integer, pnum As clsFormulaProdNum, ft As clsFormulaTable) As clsSupplyTable
    Dim st As New clsSupplyTable
    Dim bmb As clsFormulaBomb
    Dim fppc As Integer
    Dim tbco As clsFormulaTobacco
    
    With st
        .llnf = ft.pfnf
        .llyf = ft.pfyf
        .llr = da
        .pfph = ft.pfph
        .llpc = pnum.getProdNumofDay(da)
        .lljspc = pnum.getTotalProdNum(da) + ft.qspc - 1
    End With
    
    st.llqspc = st.lljspc - st.llpc + 1
    
    '异常情况处理 生产批次超出计划批次
    If pnum.getTotalProdNum(da) > ft.zpc Then
        MsgBox ft.pfph & "配方的投料批次大于计划批次，存在异常！！！"
        st.llpc = st.llpc - (pnum.getTotalProdNum(da) - ft.zpc)
        st.llpc = IIf(st.llpc < 0, 0, st.llpc)
        If st.llpc = 0 Then
            Exit Function
        End If
        st.lljspc = pnum.getTotalProdNum(da - 1) + st.llpc + ft.qspc - 1
    End If
    
    '依次轮询各bomb
    For Each bmb In ft.bombs
        If bmb.endBatch < st.llqspc Then 'bomb已使用完成
            bmb.execBatch = bmb.endBatch 'bmb.endBatch - bmb.startBatch + 1
            'do nothing
        ElseIf bmb.startBatch > st.lljspc Then 'bomb未开始
            bmb.execBatch = 0
            'do nothing
        Else
            If bmb.endBatch >= st.lljspc And bmb.startBatch <= st.llqspc Then '在1个bomb内
                fppc = st.llpc
                bmb.execBatch = st.lljspc
            ElseIf bmb.startBatch >= st.llqspc And bmb.endBatch <= st.lljspc Then '横跨1个bomb
                fppc = bmb.batchCount
                bmb.execBatch = bmb.endBatch
            ElseIf bmb.startBatch >= st.llqspc And bmb.startBatch <= st.lljspc Then '与bomb交错1
                fppc = st.lljspc - bmb.startBatch + 1
                bmb.execBatch = st.lljspc
            ElseIf bmb.endBatch >= st.llqspc And bmb.endBatch <= st.lljspc Then '与bomb交错2
                fppc = bmb.endBatch - st.llqspc + 1
                bmb.execBatch = bmb.endBatch
            End If

            '轮询添加所有烟叶
            For Each tbco In bmb.tobaccos
                st.addTobacco dbiz_supplyTobacco(tbco, fppc)
            Next
        End If
        
        '更新页面中各bomb的已投料批次
        dorg_modifySuppliedNum st.pfph, bmb
        
    Next
    
    Set dbiz_supplyTable = st
End Function

'领料表中单个烟叶计算
Public Function dbiz_supplyTobacco(ByRef tbco As clsFormulaTobacco, pc As Integer) As clsSupplyTobacco
    Dim stb As New clsSupplyTobacco
    
    With stb
        .yy = tbco.yy
        .pc = tbco.pc
        .num = pc * tbco.dpxs
    End With
    
    Set dbiz_supplyTobacco = stb
    
End Function


'月计划批次、领料情况处理
Public Function dbiz_prodPlan()
    
    Dim fmtc As Collection
    
    Set fmtc = gdGetFormulaTable
    
    dorg_prodPlan fmtc
    
End Function

