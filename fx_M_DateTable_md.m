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

/*( 
  StartDateParameter        as date, 
  EndDateParameter          as date, 
  CultureParameter          as nullable text, 
  IsRuHolidaysParameter     as nullable logical
) =>*/

let

 StartDateParameter = #date(2015, 1, 1), 
 //EndDateParameter = Date.AddDays(DateTime.Date(DateTime.FixedLocalNow()),90), 
 EndDateParameter = #date(2019, 12, 31), 
 CultureParameter = "RU-ru",
 IsRuHolidaysParameter = 0,

    /*
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
*/

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

///////// start inserting columns //////////////////////
    
    InsertYear = 
      Table.AddColumn( ChangedType, "Calendar Year", each 
        Date.Year( [Date] ), 
        Int64.Type
      ),

    
    InsertQuarter = 
      Table.AddColumn( InsertYear, "Calendar QuarterNumber", each 
        Date.QuarterOfYear( [Date] ),
        Int64.Type
      ),
    
    InsertMonth = 
      Table.AddColumn( InsertQuarter, "Calendar MonthNumber", each
        Date.Month( [Date] ),
        Int64.Type 
      ),
    
    InsertDay = 
      Table.AddColumn( InsertMonth, "Day of Month", each
        Date.Day( [Date] ),
        Int64.Type
      ),

    InsertDayName = 
      Table.AddColumn(InsertDay, "Day Name", each 
        Date.ToText( [Date], "dd", CultureParameter ),
        type text
      ),
    
    InsertDayInt = 
      Table.AddColumn( InsertDayName, "DateKey", each 
        [Calendar Year] * 10000 + [Calendar MonthNumber] * 100 + [Day of Month],
        Int64.Type
      ),
    
    InsertYearMonthNumber = 
      Table.AddColumn( InsertDayInt, "Calendar YearMonthNumber", each 
        [Calendar Year] * 100 + [Calendar MonthNumber] * 1,
        Int64.Type
      ),
    
    InsertMonthName = 
      Table.AddColumn( InsertYearMonthNumber, "Calendar Month", each 
        Date.ToText( [Date], "MMMM", CultureParameter ),
        type text
      ),

    InsertMonthDays = 
      Table.AddColumn( InsertMonthName, "Calendar MonthDays", each 
        Date.DaysInMonth([Date]),
        Int64.Type
      ),
    
    InsertCalendarMonth = 
      Table.AddColumn( InsertMonthDays, "Calendar Month Year", each
        ( try ( Text.Range( [Calendar Month], 0, 3 ) ) otherwise [Calendar Month] ) & " " & Number.ToText( [Calendar Year] ),
        type text
      ),
    
    InsertCalendarQtrYear = 
      Table.AddColumn( InsertCalendarMonth, "Calendar Quarter Year", each 
        "Q" & Number.ToText( [Calendar QuarterNumber] ) & " " & Number.ToText( [Calendar Year] ),
        type text
      ),

    InsertCalendarQtr = 
      Table.AddColumn( InsertCalendarQtrYear, "Calendar Quarter", each 
        "Q" & Number.ToText( [Calendar QuarterNumber] ),
        type text
      ),
    
    InsertQuartersDays = 
      Table.AddColumn( InsertCalendarQtr, "Calendar QuarterDays", each 
        Number.From(Date.EndOfQuarter([Date])) - Number.From(Date.StartOfQuarter([Date])) + 1,
        Int64.Type
      ),
    
    InsertDayWeek = 
      Table.AddColumn(InsertQuartersDays, "WeekDayNumber", each 
        Date.DayOfWeek( [Date], Day.Monday ) + 1, 
        Int64.Type
      ),
    
    
    // Not in DAX template
    InsertWeekEnding = 
      Table.AddColumn(InsertDayWeek, "Calendar WeekEnding", each 
        Date.EndOfWeek( [Date], Day.Monday ),
        type date
      ),
    
    // Not in DAX template
    InsertedStartofWeek = 
      Table.AddColumn(InsertWeekEnding, "Calendar StartOfWeek", each 
        Date.StartOfWeek( [Date], Day.Monday ),
        type date
      ),
    
    InsertedStartofMonth = 
      Table.AddColumn(InsertedStartofWeek, "Calendar StartOfMonth", each 
        Date.StartOfMonth( [Date] ),
        type date
      ),

    InsertedStartofQuarter = 
      Table.AddColumn(InsertedStartofMonth, "Calendar StartOfQuarter", each 
        Date.StartOfQuarter( [Date] ),
        type date
      ),

    InsertedStartofYear = 
      Table.AddColumn(InsertedStartofQuarter, "Calendar StartOfYear", each 
        Date.StartOfYear( [Date] ),
        type date
      ),
    
    InsertedEndofWeek = 
      Table.AddColumn(InsertedStartofYear, "Calendar EndOfWeek", each 
        Date.EndOfWeek( [Date] ),
        type date
      ),

    InsertedEndofMonth = 
      Table.AddColumn(InsertedEndofWeek, "Calendar EndOfMonth", each 
        Date.EndOfMonth( [Date] ),
        type date
      ),

    InsertedEndofQuarter = 
      Table.AddColumn(InsertedEndofMonth, "Calendar EndOfQuarter", each 
        Date.EndOfQuarter( [Date] ),
        type date
      ),

    InsertedEndofYear = 
      Table.AddColumn(InsertedEndofQuarter, "Calendar EndOfYear", each 
        Date.EndOfYear( [Date] ),
        type date
      ),

    InsertWeekofYear = 
      Table.AddColumn(InsertedEndofYear, "Calendar WeekNumber", each 
        Date.WeekOfYear( [Date], Day.Monday ),
        Int64.Type
      ),

/////////////week week week /////////////////////////////////////
    InsertCalendarWeekYear = 
      Table.AddColumn( InsertWeekofYear, "Calendar Week Year", each 
        "W" & Text.PadStart(Text.From([Calendar WeekNumber]),2,"0") & "-" & Number.ToText( [Calendar Year] ),
        type text
      ),

    InsertCalendarWeek = 
      Table.AddColumn( InsertCalendarWeekYear, "Calendar Week", each 
        "W" & Text.PadStart(Text.From([Calendar WeekNumber]),2,"0"),
        type text
      ),
    
    InsertDayofYear = 
      Table.AddColumn(InsertCalendarWeek, "Sequential365DayNumber", each 
        Date.DayOfYear( [Date] ),
        Int64.Type
        ),
    
    listBufferMonths = 
      List.Buffer(
        List.Distinct(
          InsertDayofYear[Calendar StartOfMonth]
        )
        ),
    
    // Not in DAX template
    AddedNumberOfMonth = 
      Table.AddColumn( InsertDayofYear, "SequentialMonthNumber", each
        List.PositionOf( listBufferMonths, [Calendar StartOfMonth]) + 1,
        Int64.Type
      ),

    // Not in DAX template
    SequentialMonthNumberReverse = 
      Table.AddColumn( AddedNumberOfMonth, "SequentialMonthNumberReverse", each
        List.PositionOf(
          List.Reverse( listBufferMonths ),
          [Calendar StartOfMonth]
        ) + 1,
        Int64.Type
      ),
    
    listBufferWeeks =
      List.Buffer( 
        List.Distinct( SequentialMonthNumberReverse[Calendar StartOfWeek] )
      ),
    
    // Not in DAX template
    AddedNumberOfWeeks = 
      Table.AddColumn( SequentialMonthNumberReverse, "SequentialWeekNumber", each
        List.PositionOf( listBufferWeeks, [Calendar StartOfWeek] ) + 1,
        Int64.Type
      ),
    // Not in DAX template
    AddedNumberOfWeeksReverse = 
      Table.AddColumn( AddedNumberOfWeeks, "SequentialWeekNumberReverse", each
        List.PositionOf( List.Reverse( listBufferWeeks ), [Calendar StartOfWeek] ) + 1,
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
    ///////////////// Previous Dates /////////////////////////////

    InsertPreviousMonthDate = 
        Table.AddColumn(
            InsertSequentialDayNumberReverse, 
            "Calendar DatePreviousMonth", each Date.AddMonths([Date], -1),
            Date.Type
        ),

    InsertPreviousQuarterDate = 
        Table.AddColumn(
            InsertPreviousMonthDate, 
            "Calendar DatePreviousQuarter", each Date.AddQuarters([Date], -1),
            Date.Type
        ),

    InsertPreviousWeekDate = 
        Table.AddColumn(
            InsertPreviousQuarterDate, 
            "Calendar DatePreviousWeek", each Date.AddWeeks([Date], -1),
            Date.Type
        ),

    InsertPreviousYearDate = 
        Table.AddColumn(
            InsertPreviousWeekDate, 
            "Calendar DatePreviousYear", each Date.AddYears([Date], -1),
            Date.Type
        ),

///////////////// Days of Period /////////////////////////////

// Calendar DayOfMonthNumber
// Calendar DayOfQuarterNumber
// Calendar DayOfYearNumber

    InsertDayOfMonthNumber = 
        Table.AddColumn(
            InsertPreviousYearDate, 
            "Calendar DayOfMonthNumber", each Number.From([Date]) - Number.From([Calendar StartOfMonth]) +1,
            Int64.Type
        ),

    InsertDayOfQuarterNumber = 
        Table.AddColumn(
            InsertDayOfMonthNumber, 
            "Calendar DayOfQuarterNumber", each Number.From([Date]) - Number.From([Calendar StartOfQuarter]) +1,
            Int64.Type
        ),

    InsertDayOfYearNumber = 
        Table.AddColumn(
            InsertDayOfQuarterNumber, 
            "Calendar DayOfYearNumber", each Date.DayOfYear([Date]),
            Int64.Type
        ),
    LastStep = InsertDayOfYearNumber

   /* 
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

    */
    
//in
  //if IsRuHolidaysParameter
  //then insertRuShortdaysColumn
  //else InsertSequentialDayNumberReverse
  //InsertSequentialDayNumberReverse,

    //#"Вызванная функцияFunCreatCal2" = FunCreatCal(#date(2013, 1, 1), #date(2019, 12, 31), "Ru-ru", true),

in
   LastStep