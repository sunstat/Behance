class DateUtilities():
    @staticmethod
    def date_filer_help_(date1, date2):
        date1_arr = date1.split("-")
        date2_arr = date2.split("-")
        for i in range(len(date1_arr)):
            if int(date1_arr[i]) < int(date2_arr[i]):
                return True
            elif int(date1_arr[i]) > int(date2_arr[i]):
                return False
        return True

    @staticmethod
    def date_filter(prev_date, date, end_date):
        return DateUtilities.date_filer_help_(prev_date, date) and DateUtilities.date_filer_help_(date, end_date)