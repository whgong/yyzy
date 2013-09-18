Attribute VB_Name = "modGetData"
''''''''''''''''''''''''''''''''''''''
'数据获取模块(用于获取excel表格中数据)
''''''''''''''''''''''''''''''''''''''

'获取牌号列表
Public Function gdPfphList() As Collection
    Dim reslist As New Collection
    
    Dim wss As Sheets
    Dim ws As Worksheet
    Dim phmc As String
    
    Set wss = ThisWorkbook.Worksheets
    
    For i = 4 To wss.Count
    
        reslist.Add wss(i).Name
        
    Next i
    
    If modGlobalVar.pfphlist Is Nothing Then
        Set modGlobalVar.pfphlist = reslist
    End If
    
    Set gdPfphList = reslist

End Function

'获取bomb中一个烟叶
Public Function gdGetOneFormulaTobacco(ByRef cel As Range) As clsFormulaTobacco
    Dim tbco As New clsFormulaTobacco
    
    tbco.yy = cel.Offset(0, 0).Value
    
    If cel.Offset(0, 1).MergeCells Then
        tbco.pc = cel.Offset(0, 1).MergeArea.Cells(1, 1).Value
    Else
        tbco.pc = cel.Offset(0, 1).Value
    End If
    
    If cel.Offset(0, 2).MergeCells Then
        tbco.dpxs = cel.Offset(0, 2).MergeArea.Cells(1, 1).Value
    Else
        tbco.dpxs = cel.Offset(0, 2).Value
    End If

    Set gdGetOneFormulaTobacco = tbco
End Function

'获取配方中1个bomb
Public Function gdGetOneFormulaBomb(ByRef cel As Range) As clsFormulaBomb
    Dim bmb As New clsFormulaBomb
    
    Dim re As New RegExp
    Dim rmc As MatchCollection
    re.Pattern = "(\d+)"
    re.Global = True
    
    If re.test(cel.Value) = True Then
        Set rmc = re.Execute(cel.Value)
        If rmc.Count < 4 Then
            cel.Activate
            MsgBox "不符合格式的配方单"
        Else
            bmb.bombId = rmc(0)
            bmb.startBatch = rmc(1)
            bmb.endBatch = rmc(2)
            bmb.batchCount = rmc(3)
        End If

    Else
        cel.Activate
        MsgBox "不符合格式的配方单"
    End If
    
    Dim i As Integer, r As Integer
    i = 2
    Do
        bmb.addTobacco gdGetOneFormulaTobacco(cel.Offset(i, 0))
        If cel.Offset(i, 0).MergeCells Then
            r = cel.Offset(i, 0).MergeArea.Rows.Count
        Else
            r = 1
        End If
        i = i + r
    Loop While cel.Offset(i, 0).Value <> ""
    
    
    Set gdGetOneFormulaBomb = bmb
End Function

'获取一个牌号的配方表
Public Function gdGetOneFormulaTable(ByVal ph As String) As clsFormulaTable
    Dim r As Integer, c As Integer
    
    Dim ws As Worksheet
    Set ws = ThisWorkbook.Worksheets(ph)
    Dim tmpstr As String
    Dim res As New clsFormulaTable
    
    Dim cel As Range
    
    r = modGlobalVar.wsp_r1
    c = modGlobalVar.wsp_c1
    
    tmpstr = ws.Cells(r, c).Value
    
    res.pfph = ph
    
    Dim re As New RegExp
    Dim rmc As MatchCollection
    re.Pattern = "(\d+)"
    re.Global = True
    If (re.test(tmpstr) = True) Then
        Set rmc = re.Execute(tmpstr)
        If rmc.Count < 2 Then
            ws.Activate
            MsgBox "不符合规则的配方单"
        Else
            res.pfnf = rmc(0)
            res.pfyf = rmc(1)
            'res.zpc = rmc(3)
        End If
    Else
        ws.Cells(r, c).Activate
        MsgBox "不符合规则的配方单"
    End If
    
    Set cel = ws.Cells(r + 1, c)
    Do
        res.addOneBomb gdGetOneFormulaBomb(cel)
        Set cel = ws.Cells(r + 1, cel.Column + 3)
    Loop While cel.Value <> ""
    
    res.zpc = res.jspc - res.qspc + 1
    
    Set gdGetOneFormulaTable = res
    
End Function

'获取各配方的配方表集合
Public Function gdGetFormulaTable() As Collection
    
    Dim reslist As New Collection
    
    Dim phlist As Collection
    Set phlist = gdPfphList()
    
    For Each ph In modGlobalVar.pfphlist
        reslist.Add Item:=gdGetOneFormulaTable(ph), Key:=ph
    Next
    
    Set gdGetFormulaTable = reslist
    
End Function

'Public Sub testGetFormulaTable()
'    Dim ft As Collection
'    Set modGlobalVar.pfphList = gdPfphList()
'    Set ft = gdGetFomulaTable()
'    MsgBox "get data done"
'End Sub


'获取单个配方的投料批次
Public Function gdGetOneFormulaProdNum(ByRef cel As Range, da As Integer, pfph As String) As clsFormulaProdNum
    Dim r As Integer, c As Integer, py As Integer
    Dim pnum As New clsFormulaProdNum
    
    Dim i As Integer
    r = modGlobalVar.ws1_r3
    c = modGlobalVar.ws1_c3
    py = modGlobalVar.ws1_c3 - modGlobalVar.ws1_c1
    pnum.pfph = pfph
    For i = py To da + py
        
        If (cel.Offset(0, i).Value < 0) Then
            MsgBox pnum.pfph & "的生产批次为负数，存在异常！！！"
        End If
        pnum.addProdNum (i - py + 1), cel.Offset(0, i).Value
        
    Next i
    
    Set gdGetOneFormulaProdNum = pnum

End Function

'获取各配方的投料批次集合
Public Function gdGetFormulaProdNum(da As Integer) As Collection
    Dim r As Integer, c As Integer
    Dim res As New Collection
    Dim ws As Worksheet
    Dim i As Integer
    
    Set ws = ThisWorkbook.Worksheets("日投料批次")
    r = modGlobalVar.ws1_r1
    c = modGlobalVar.ws1_c1
    i = r
    Do
        res.Add Item:=gdGetOneFormulaProdNum(ws.Cells(i, c), da, ws.Cells(i, c).Value), Key:=ws.Cells(i, c)
        i = i + 1
    Loop While ws.Cells(i, c).Value <> ""
    
    Set gdGetFormulaProdNum = res

End Function

'Public Sub test()
'    gdGetOneFormulaTable ("2#膨丝")
'    MsgBox "test"
'
'End Sub

