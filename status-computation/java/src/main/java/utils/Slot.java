package utils;
public class Slot{
		public int time_int;
		public int date_int;
		public String timestamp;
		public String status;
		public String prev_status;
		
		public Slot(int _time_int, int _date_int, String _timestamp, String _status, String _prev_status){
			this.time_int = _time_int;
			this.date_int = _date_int;
			this.timestamp = _timestamp;
			this.status = _status;
			this.prev_status = _prev_status;
			
		}
	}