// killerDateTableRu
// функция killerDateTableRu создает таблицу с датами. Дополняет ее различными полезными для Time Intelligence столбцами. 
// В случае, если четвертый аргумент принимает Дни 1, то функция добавляет столбец с выходными.
// Оригинальная идея philbritton https://gist.github.com/philbritton/9677152
// пример: killerDateTableRu(#date(2017, 2, 1), #date(2017, 2, 4), "RU-ru", 1)

// Описание полей результирующей таблицы:
// ===================================================
// Date - дата
// Year - год
// QuarterOfYear - номер квартала
// MonthOfYear - номер месяца в году
// MonthDayNumber - номер дня в месяце
// DateInt - дата в форме целого числа
// YearMonthNumber - МесяцГод в форме 201701
// MonthName - название месяца в формате Январь
// MonthInCalendar - Месяц год в формате Янв 2017
// QuarterInCalendar - КварталГод в формате Q1 2017
// DayInWeek - номер дня недели
// DayOfWeekName - название дня недели
// WeekEnding - дата окончания недели
// StartOfWeek - дата начала недели
// StartOfMonth - дата начала месяца
// WeekOfYear - номер недели в году
// DayOfYear - номер дня от начала года
// SequentialMonthNumber - номер месяца в таблице по порядку
// SequentialMonthNumberReverse - номер месяца в таблице в обратном порядке
// SequentialWeekNumber - номер недели в таблице по порядку
// SequentialWeekNumberReverse - номер недели в таблице в обратном порядке
// SequentialDayNumber - номер дня в таблице по порядку
// SequentialDayNumberReverse - номер дня в таблице в обратном порядке

( 
  StartDateParameter        as date, 
  EndDateParameter          as date, 
  CultureParameter          as nullable text, 
  IsRuHolidaysParameter     as nullable logical
) =>

let
    MONTHS_ORDERED_BY_DEFAULT = {
        "Январь", "Февраль", "Март",
        "Апрель", "Май", "Июнь", 
        "Июль", "Август", "Сентябрь", 
        "Октябрь", "Ноябрь", "Декабрь"
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

          #"Повышенные заголовки" = 
            Table.PromoteHeaders( GetDataFromGovRu ),
          
          #"Переименовали поле в год" = 
            Table.RenameColumns( #"Повышенные заголовки",
              { 
                {"Год/Месяц", "Год"}
              }
            ),

          #"Выбрали год и месяц" =
            Table.SelectColumns( #"Переименовали поле в год", 
              { "Год" } & MONTHS_ORDERED_BY_DEFAULT 
            ),

          #"Спрямили календарь" = 
            Table.UnpivotOtherColumns( #"Выбрали год и месяц", 
              { "Год" }, "Месяц", "День"
            ),
          
          #"Добавили номер месяца" = 
            Table.AddColumn( #"Спрямили календарь", "НомерМесяца", each 
              1 + List.PositionOf( MONTHS_ORDERED_BY_DEFAULT, [Месяц] ), 
              type number
            ),
          
          #"Преобразовали строку дат в список" =
            Table.TransformColumns( #"Добавили номер месяца",
              { 
                { "День", each Text.Split( _, "," ) }
              }
            ),

          #"Развернули список дней" = 
            Table.ExpandListColumn( #"Преобразовали строку дат в список",
              "День" 
            ),

          #"Добавили сокращенный день" = 
            Table.AddColumn( #"Развернули список дней", "Сокращенный День", each 
              Text.Contains( [День], "*" )
            ),

          #"Добавили выходной день" = 
            Table.AddColumn( #"Добавили сокращенный день", "Выходной День", each 
              not [Сокращенный День]
            ),

          #"Удалили признак сокращенного дня" = 
            Table.ReplaceValue( #"Добавили выходной день",
              "*", "", Replacer.ReplaceText, { "День" }
            ),

          #"Уточнли тип полей календаря" = 
            Table.TransformColumnTypes( #"Удалили признак сокращенного дня",
              {
                { "Год", Int64.Type },
                { "День", Int64.Type },
                { "НомерМесяца", Int64.Type }
              }
            ),

          #"Рассчитали дату" = 
            Table.AddColumn( #"Уточнли тип полей календаря", "Дата", each
              #date( [Год], [НомерМесяца], [День] )
            )
      in
          #"Рассчитали дату",

// Примечание: 
//  источник взят отсюда: http://data.gov.ru/opendata/7708660670-proizvcalendar
//  На момент 24.05.2018 в календаре содержатся описание праздников с 1999 года по 2025 год.
    PROD_СALENDAR_URL =
      "http://data.gov.ru/opendata/7708660670-proizvcalendar/data-20180410T1145-structure-20180410T1145.csv?encoding=UTF-8",

    ProductionCalendar = getProductionCalendar( PROD_СALENDAR_URL ),

    ListOfShortday = 
      Table.SelectRows( ProductionCalendar, each [Сокращенный День] = true )[Дата],
    
    ListOfHoliday =
      Table.SelectRows( ProductionCalendar, each [Выходной День] = true )[Дата],

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