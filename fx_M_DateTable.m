// killerDateTableRu
// ������� killerDateTableRu ������� ������� � ������. ��������� �� ���������� ��������� ��� Time Intelligence ���������. 
// � ������, ���� ��������� �������� ��������� ��� 1, �� ������� ��������� ������� � ���������.
// ������������ ���� philbritton https://gist.github.com/philbritton/9677152
// ������: killerDateTableRu(#date(2017, 2, 1), #date(2017, 2, 4), "RU-ru", 1)

// �������� ����� �������������� �������:
// ===================================================
// Date - ����
// Year - ���
// QuarterOfYear - ����� ��������
// MonthOfYear - ����� ������ � ����
// MonthDayNumber - ����� ��� � ������
// DateInt - ���� � ����� ������ �����
// YearMonthNumber - �������� � ����� 201701
// MonthName - �������� ������ � ������� ������
// MonthInCalendar - ����� ��� � ������� ��� 2017
// QuarterInCalendar - ���������� � ������� Q1 2017
// DayInWeek - ����� ��� ������
// DayOfWeekName - �������� ��� ������
// WeekEnding - ���� ��������� ������
// StartOfWeek - ���� ������ ������
// StartOfMonth - ���� ������ ������
// WeekOfYear - ����� ������ � ����
// DayOfYear - ����� ��� �� ������ ����
// SequentialMonthNumber - ����� ������ � ������� �� �������
// SequentialMonthNumberReverse - ����� ������ � ������� � �������� �������
// SequentialWeekNumber - ����� ������ � ������� �� �������
// SequentialWeekNumberReverse - ����� ������ � ������� � �������� �������
// SequentialDayNumber - ����� ��� � ������� �� �������
// SequentialDayNumberReverse - ����� ��� � ������� � �������� �������

( 
  StartDateParameter        as date, 
  EndDateParameter          as date, 
  CultureParameter          as nullable text, 
  IsRuHolidaysParameter     as nullable logical
) =>

let
    MONTHS_ORDERED_BY_DEFAULT = {
        "������", "�������", "����",
        "������", "���", "����", 
        "����", "������", "��������", 
        "�������", "������", "�������"
      },

    getProductionCalendar = ( URL as text ) as nullable table =>
      let
          GetDataFromGovRu =
            Csv.Document( 
              Web.Contents( URL ),
              [ Delimiter  = ",",
                Encoding   = 65001,
                QuoteStyle = QuoteStyle.None ]
            ),

          #"���������� ���������" = 
            Table.PromoteHeaders( GetDataFromGovRu ),
          
          #"������������� ���� � ���" = 
            Table.RenameColumns( #"���������� ���������",
              { 
                {"���/�����", "���"}
              }
            ),

          #"������� ��� � �����" =
            Table.SelectColumns( #"������������� ���� � ���", 
              { "���" } & MONTHS_ORDERED_BY_DEFAULT 
            ),

          #"�������� ���������" = 
            Table.UnpivotOtherColumns( #"������� ��� � �����", 
              { "���" }, "�����", "����"
            ),
          
          #"�������� ����� ������" = 
            Table.AddColumn( #"�������� ���������", "�����������", each 
              1 + List.PositionOf( MONTHS_ORDERED_BY_DEFAULT, [�����] ), 
              type number
            ),
          
          #"������������� ������ ��� � ������" =
            Table.TransformColumns( #"�������� ����� ������",
              { 
                { "����", each Text.Split( _, "," ) }
              }
            ),

          #"���������� ������ ����" = 
            Table.ExpandListColumn( #"������������� ������ ��� � ������",
              "����" 
            ),

          #"�������� ����������� ����" = 
            Table.AddColumn( #"���������� ������ ����", "����������� ����", each 
              Text.Contains( [����], "*" )
            ),

          #"�������� �������� ����" = 
            Table.AddColumn( #"�������� ����������� ����", "�������� ����", each 
              not [����������� ����]
            ),

          #"������� ������� ������������ ���" = 
            Table.ReplaceValue( #"�������� �������� ����",
              "*", "", Replacer.ReplaceText, { "����" }
            ),

          #"������� ��� ����� ���������" = 
            Table.TransformColumnTypes( #"������� ������� ������������ ���",
              {
                { "���", Int64.Type },
                { "����", Int64.Type },
                { "�����������", Int64.Type }
              }
            ),

          #"���������� ����" = 
            Table.AddColumn( #"������� ��� ����� ���������", "����", each
              #date( [���], [�����������], [����] )
            )
      in
          #"���������� ����",

// ����������: 
//  �������� ���� ������: http://data.gov.ru/opendata/7708660670-proizvcalendar
//  �� ������ 24.05.2018 � ��������� ���������� �������� ���������� � 1999 ���� �� 2025 ���.
    PROD_�ALENDAR_URL =
      "http://data.gov.ru/opendata/7708660670-proizvcalendar/data-20180410T1145-structure-20180410T1145.csv?encoding=UTF-8",

    ProductionCalendar = getProductionCalendar( PROD_�ALENDAR_URL ),

    ListOfShortday = 
      Table.SelectRows( ProductionCalendar, each [����������� ����] = true )[����],
    
    ListOfHoliday =
      Table.SelectRows( ProductionCalendar, each [�������� ����] = true )[����],

    CheckIfThereIsShortdayList = 
      List.Buffer( 
        try ListOfShortday otherwise {} 
      ),

    CheckIfThereIsHolidayList = 
      List.Buffer( 
        try ListOfHoliday otherwise {} 
      ),

    DayCount = 
      Duration.Days( 
        Duration.From( 
          EndDateParameter - StartDateParameter 
        ) 
      ) + 1,
    
    MainList = 
      List.Dates( StartDateParameter, DayCount, #duration( 1, 0, 0, 0 ) ),
    
    TableFromList = 
      Table.FromList( MainList, 
        Splitter.SplitByNothing(), { "Date" } 
      ),    
    
    ChangedType = 
      Table.TransformColumnTypes( TableFromList, 
        { 
          {"Date", type date } 
        } 
      ),
    
    InsertYear = 
      Table.AddColumn( ChangedType, "Year", each 
        Date.Year( [Date] ), 
        Int64.Type
      ),
    
    InsertQuarter = 
      Table.AddColumn( InsertYear, "QuarterOfYear", each 
        Date.QuarterOfYear( [Date] ),
        Int64.Type
      ),
    
    InsertMonth = 
      Table.AddColumn( InsertQuarter, "MonthOfYear", each
        Date.Month( [Date] ),
        Int64.Type 
      ),
    
    InsertDay = 
      Table.AddColumn( InsertMonth, "MonthDayNumber", each
        Date.Day( [Date] ),
        Int64.Type
      ),
    
    InsertDayInt = 
      Table.AddColumn( InsertDay, "DateInt", each 
        [Year] * 10000 + [MonthOfYear] * 100 + [MonthDayNumber],
        Int64.Type
      ),
    
    InsertYearMonthNumber = 
      Table.AddColumn( InsertDayInt, "YearMonthNumber", each 
        [Year] * 100 + [MonthOfYear] * 1,
        Int64.Type
      ),
    
    InsertMonthName = 
      Table.AddColumn( InsertYearMonthNumber, "MonthName", each 
        Date.ToText( [Date], "MMMM", CultureParameter ),
        type text
      ),
    
    InsertCalendarMonth = 
      Table.AddColumn( InsertMonthName, "MonthInCalendar", each
        ( try ( Text.Range( [MonthName], 0, 3 ) ) otherwise [MonthName] ) & " " & Number.ToText( [Year] ),
        type text
      ),
    
    InsertCalendarQtr = 
      Table.AddColumn( InsertCalendarMonth, "QuarterInCalendar", each 
        "Q" & Number.ToText( [QuarterOfYear] ) & " " & Number.ToText( [Year] ),
        type text
      ),
    
    InsertDayWeek = 
      Table.AddColumn(InsertCalendarQtr, "DayInWeek", each 
        Date.DayOfWeek( [Date], Day.Monday ) + 1, 
        Int64.Type
      ),
    
    InsertDayName = 
      Table.AddColumn(InsertDayWeek, "DayOfWeekName", each 
        Date.ToText( [Date], "dddd", CultureParameter ),
        type text
      ),
    
    InsertWeekEnding = 
      Table.AddColumn(InsertDayName, "WeekEnding", each 
        Date.EndOfWeek( [Date], Day.Monday ),
        type date
      ),
    
    InsertedStartofWeek = 
      Table.AddColumn(InsertWeekEnding, "StartOfWeek", each
        Date.StartOfWeek( [Date], Day.Monday ),
        type date
      ),
    
    InsertedStartofMonth = 
      Table.AddColumn(InsertedStartofWeek, "StartOfMonth", each 
        Date.StartOfMonth( [Date] ),
        type date
      ),
    
    InsertWeekofYear = 
      Table.AddColumn(InsertedStartofMonth, "WeekOfYear", each 
        Date.WeekOfYear( [Date], Day.Monday ),
        Int64.Type
      ),
    
    InsertDayofYear = 
      Table.AddColumn(InsertWeekofYear, "DayOfYear", each 
        Date.DayOfYear( [Date] ),
        Int64.Type
        ),
    
    listBufferMonths = 
      List.Buffer(
        List.Distinct(
          InsertDayofYear[StartOfMonth]
        )
        ),
    
    AddedNumberOfMonth = 
      Table.AddColumn( InsertDayofYear, "SequentialMonthNumber", each
        List.PositionOf( listBufferMonths, [StartOfMonth]) + 1,
        Int64.Type
      ),
    
    SequentialMonthNumberReverse = 
      Table.AddColumn( AddedNumberOfMonth, "SequentialMonthNumberReverse", each
        List.PositionOf(
          List.Reverse( listBufferMonths ),
          [StartOfMonth]
        ) + 1,
        Int64.Type
      ),
    
    listBufferWeeks =
      List.Buffer( 
        List.Distinct( SequentialMonthNumberReverse[StartOfWeek] )
      ),
    
    AddedNumberOfWeeks = 
      Table.AddColumn( SequentialMonthNumberReverse, "SequentialWeekNumber", each
        List.PositionOf( listBufferWeeks, [StartOfWeek] ) + 1,
        Int64.Type
      ),

    AddedNumberOfWeeksReverse = 
      Table.AddColumn( AddedNumberOfWeeks, "SequentialWeekNumberReverse", each
        List.PositionOf( List.Reverse( listBufferWeeks ), [StartOfWeek] ) + 1,
        Int64.Type
      ),
    
    InsertSequentialDayNumber =
      Table.AddIndexColumn(
        AddedNumberOfWeeksReverse,
        "SequentialDayNumber",
        1,
        1
      ),

    InsertSequentialDayNumberReverse =
      Table.AddIndexColumn( 
        InsertSequentialDayNumber,
        "SequentialDayNumberReverse",
        List.Max( InsertSequentialDayNumber[SequentialDayNumber] ),
        -1 
      ),

    insertRuHolidaysColumn = 
      Table.AddColumn( InsertSequentialDayNumberReverse, "Holiday", each
        if List.Count( CheckIfThereIsHolidayList ) = 0 
        then null
        else 
          if List.Contains( CheckIfThereIsHolidayList, [Date]) 
          then true 
          else false
      ),

    insertRuShortdaysColumn = 
      Table.AddColumn( insertRuHolidaysColumn, "Shortday", each
        if List.Count( CheckIfThereIsShortdayList ) = 0 
        then null
        else 
          if List.Contains( CheckIfThereIsShortdayList, [Date]) 
          then true 
          else false
      )
    
in
  if IsRuHolidaysParameter
  then insertRuShortdaysColumn
  else InsertSequentialDayNumberReverse