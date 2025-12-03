CREATE INDEX stopname_idx ON stops(stop_name);

CREATE OR REPLACE PROCEDURE swap_buses(r_num_from varchar(7),r_num_to varchar(7),n int)
LANGUAGE SQL AS $$
	WITH target_b AS (SELECT bus_id FROM buses WHERE route_num=r_num_from LIMIT n)
	UPDATE buses SET route_num=r_num_to WHERE bus_id IN (SELECT bus_id FROM target_b);
$$;

CREATE OR REPLACE PROCEDURE add_or_mod_route(r_num varchar(7), term_stop1 varchar(50), term_stop2 varchar(50))
LANGUAGE PLPGSQL AS $$
DECLARE
	route record;
	stop1 record;
	stop2 record;
BEGIN
	SELECT stop_id INTO stop1 FROM stops
	WHERE stop_name=term_stop1
	ORDER BY stop_id
	LIMIT 1;
	IF NOT FOUND THEN
		RAISE NOTICE 'Остановка % не найдена',term_stop1;
	ELSE
		SELECT stop_id INTO stop2 FROM stops
		WHERE stop_name=term_stop2
		ORDER BY stop_id
		LIMIT 1;
		IF NOT FOUND THEN
			RAISE NOTICE 'Остановка % не найдена',term_stop2;
		ELSE
			SELECT route_num INTO route FROM routes
			WHERE route_num = r_num;
			IF NOT FOUND THEN
				INSERT INTO routes(route_num,terminal_stop1,terminal_stop2)
				VALUES	(r_num,stop1.stop_id,stop2.stop_id);
				RAISE NOTICE 'Добавлен маршрут %: % - %',r_num,stop1.stop_id,stop2.stop_id;
			ELSE
				SELECT terminal_stop1,terminal_stop2 INTO route FROM routes
				WHERE route_num = r_num;
				IF route.terminal_stop1=stop1.stop_id AND route.terminal_stop2=stop2.stop_id THEN
					RAISE NOTICE 'Маршрут % не изменён',r_num;
				ELSE 
					UPDATE routes SET terminal_stop1=stop1.stop_id, terminal_stop2=stop2.stop_id
					WHERE route_num = r_num;
					RAISE NOTICE 'Изменены конечные остановки маршрута %',r_num;
				END IF;
			END IF;
		END IF;
	END IF;
END $$;

CREATE OR REPLACE PROCEDURE insert_stop_to_trip(tr_id int, st_id int, order_num int)
LANGUAGE SQL AS $$
	UPDATE trips_stops SET stop_order=stop_order+1
	WHERE stop_order>=order_num AND trip_id=tr_id;
	INSERT INTO trips_stops(trip_id,stop_id,stop_order)
	VALUES (tr_id,st_id,order_num);
$$;

CREATE OR REPLACE PROCEDURE define_trip(tr_id int,stop_ids int[])
LANGUAGE PLPGSQL AS $$
DECLARE
	i int:=0;
BEGIN
	DELETE FROM trips_stops WHERE trip_id=tr_id;
	FOR i IN 1..array_length(stop_ids,1) LOOP
		INSERT INTO trips_stops(trip_id,stop_id,stop_order)
		VALUES (tr_id,stop_ids[i],i);
	END LOOP;
END $$;

CREATE OR REPLACE PROCEDURE add_driver_work_h(lic_id bigint, work_amount numeric(9,4))
LANGUAGE SQL AS $$
	UPDATE drivers SET work_hours=work_hours+work_amount
	WHERE license_id=lic_id;
$$;

CREATE OR REPLACE FUNCTION minutes_betw_stops(stid1 int, stid2 int) RETURNS numeric
LANGUAGE PLPGSQL AS $$
DECLARE
	lat1 float;
	lat2 float;
	lat_delta float;
	long_delta float;
	dist float;
BEGIN
	lat1:=(SELECT latitude FROM stops WHERE stop_id=stid1)*(3.1416/180);
	lat2:=(SELECT latitude FROM stops WHERE stop_id=stid2)*(3.1416/180);
	lat_delta:=lat2-lat1;
	long_delta:=((SELECT longitude FROM stops WHERE stop_id=stid2)-(SELECT longitude FROM stops WHERE stop_id=stid1))*(3.1416/180);
	dist := 6371*sqrt(power(lat_delta,2)+power(cos((lat2+lat1)/2)*long_delta,2));
	RETURN round((dist/30*60)::numeric,1);
END $$;

CREATE OR REPLACE FUNCTION add_arr_time() RETURNS TRIGGER
LANGUAGE PLPGSQL AS $$
DECLARE
	stop_ids int[];
	sti record;
	minu_from_dep numeric;
	arr_t time;
	tr_st_id int;
	i int;
BEGIN
	i:=1;
	FOR sti IN SELECT stop_id FROM trips_stops
				WHERE trip_id=NEW.trip_id
				ORDER BY stop_order LOOP
		stop_ids[i]:=sti.stop_id;
		i:=i+1;
	END LOOP;
	minu_from_dep := 0;
	arr_t := NEW.departure_time;
	FOR i IN 1..array_length(stop_ids,1) LOOP
		IF i>1 THEN
			minu_from_dep := minu_from_dep+minutes_betw_stops(stop_ids[i-1],stop_ids[i]);
		END IF;
		arr_t := NEW.departure_time+(round(minu_from_dep)::text||' minute')::interval;
		tr_st_id := (SELECT trip_stop_id FROM trips_stops
		WHERE trip_id=NEW.trip_id AND stop_id=stop_ids[i] LIMIT 1);
		INSERT INTO stops_arrivals(trip_stop_id,arrival_time) VALUES (tr_st_id, arr_t);
	END LOOP;
	RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION del_arr_time() RETURNS TRIGGER
LANGUAGE PLPGSQL AS $$
DECLARE
	stop_ids int[];
	sti record;
	minu_from_dep numeric;
	old_arr_t time;
	tr_st_id int;
	i int;
BEGIN
	i:=1;
	FOR sti IN SELECT stop_id FROM trips_stops
				WHERE trip_id=OLD.trip_id
				ORDER BY stop_order LOOP
		stop_ids[i]:=sti.stop_id;
		i:=i+1;
	END LOOP;
	minu_from_dep := 0;
	old_arr_t := OLD.departure_time;
	FOR i IN 1..array_length(stop_ids,1) LOOP
		IF i>1 THEN
			minu_from_dep := minu_from_dep+minutes_betw_stops(stop_ids[i-1],stop_ids[i]);
		END IF;
		old_arr_t := OLD.departure_time+(round(minu_from_dep)::text||' minute')::interval;
		tr_st_id := (SELECT trip_stop_id FROM trips_stops
		WHERE trip_id=OLD.trip_id AND stop_id=stop_ids[i] LIMIT 1);
		DELETE FROM stops_arrivals
		WHERE trip_stop_id=tr_st_id AND arrival_time=old_arr_t;
	END LOOP;
	RETURN OLD;
END $$;

CREATE OR REPLACE TRIGGER calc_stop_arrival_times
AFTER INSERT ON trips_departures
FOR EACH ROW
EXECUTE PROCEDURE add_arr_time();

CREATE OR REPLACE TRIGGER clear_stop_arrivals
BEFORE DELETE ON trips_departures
FOR EACH ROW
EXECUTE PROCEDURE del_arr_time();

CREATE OR REPLACE FUNCTION trunc_arr_time() RETURNS TRIGGER
LANGUAGE PLPGSQL AS $$
DECLARE
BEGIN
	TRUNCATE stops_arrivals;
	RETURN OLD;
END $$;

CREATE OR REPLACE TRIGGER trunc_stop_arrivals
AFTER TRUNCATE ON trips_departures
EXECUTE PROCEDURE trunc_arr_time();

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(1,'41-й километр',56.016629,37.194423,true),
		(2,'Панфиловский проспект',55.983999,37.17985,true),
		(3,'МЦД Зеленоград-Крюково',55.980989,37.17496,true),
		(4,'Универсам',55.98081,37.184073,false),
		(5,'МЖК',55.992736,37.252537,true),
		(6,'Городское кладбище',55.983953,37.258299,false),
		(7,'Городская больница',55.984679,37.235155,true),
		(8,'Алабушевское кладбище',56.011984,37.139502,true),
		(9,'10-й микрорайон',55.983373,37.175802,false),
		(10,'Северная',56.013321,37.189902,true),
		(11,'14-й микрорайон',55.988505,37.145349,false),
		(12,'Улица Болдов Ручей',55.99552,37.181103,false),
		(13,'МЦД Фирсановская',55.960886,37.25193,false),
		(14,'Кинотеатр Электрон',56.003043,37.207259,false);

CALL add_or_mod_route('4','МЦД Зеленоград-Крюково','Улица Болдов Ручей');
CALL add_or_mod_route('8','41-й километр','Панфиловский проспект');
CALL add_or_mod_route('29','МЦД Зеленоград-Крюково','Кинотеатр Электрон');
CALL add_or_mod_route('12','МЦД Зеленоград-Крюково','МЖК');
CALL add_or_mod_route('2к','МЦД Зеленоград-Крюково','Городское кладбище');
CALL add_or_mod_route('31','МЦД Зеленоград-Крюково','Городская больница');
CALL add_or_mod_route('3','МЦД Зеленоград-Крюково','Алабушевское кладбище');
CALL add_or_mod_route('9','41-й километр','МЦД Зеленоград-Крюково');
CALL add_or_mod_route('27','Алабушевское кладбище','МЦД Фирсановская');
CALL add_or_mod_route('11','Северная','МЦД Зеленоград-Крюково');
CALL add_or_mod_route('19','14-й микрорайон','Городская больница');
CALL add_or_mod_route('23','14-й микрорайон','Северная');

INSERT INTO buses(bus_id,route_num,model,capacity)
VALUES	('РО63277', '8','НефАЗ 5299-40-52',103),
		('СК13777','29','НефАЗ 5299-40-52',103),
		('РО55577','29','НефАЗ 5299-40-52',103),
		('СК11677','12','НефАЗ 5299-40-52',103),
		('РО59877','2к','НефАЗ 5299-40-52',103),
		('ОО67877','2к','НефАЗ 5299-40-52',111),
		('РО52577','31','НефАЗ 5299-40-52',103),
		('МУ08577','31','НефАЗ 5299-40-52',103),
		('СК02677', '3','НефАЗ 5299-40-52',103),
		('РО67177', '9','НефАЗ 5299-40-52',103),
		('РО60377', '4','НефАЗ 5299-40-52',103),
		('РО58077','27','НефАЗ 5299-40-52',103),
		('РО52977','11','НефАЗ 5299-40-52',103),
		('ОО67377','19','НефАЗ 5299-40-52',103);

INSERT INTO trips(trip_id,route_num,forward)
VALUES 	(801,'8',true),(800,'8',false),
		(291,'29',true),(290,'29',false),
		(121,'12',true),(120,'12',false),
		(201,'2к',true),(200,'2к',false),
		(311,'31',true),(310,'31',false),
		(301,'3',true),(300,'3',false),
		(901,'9',true),(900,'9',false),
		(401,'4',true),(400,'4',false),
		(271,'27',true),(270,'27',false),
		(111,'11',true),(110,'11',false),
		(191,'19',true),(190,'19',false);

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(15,'12-й микрорайон',55.995483,37.190087,true),
		(16,'1-й торговый центр',56.006266,37.203491,false),
		(17,'Магазин Океан',55.997546,37.211135,false),
		(18,'Магазин Товары для дома',55.992882,37.213747,false),
		(19,'МИЭТ',55.983341,37.210652,false);
CALL define_trip(801,ARRAY[2,15,1]);
CALL define_trip(800,ARRAY[1,10,16,14,17,18,19,4,2]);

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(20,'Корпус № 1012',55.988035,37.169064,false);
CALL define_trip(401,ARRAY[3,2,15,12]);
CALL define_trip(400,ARRAY[12,20,9,3]);

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(21,'10-й микрорайон',55.983728,37.175661,true),
		(22,'Корпус № 1012',55.988809,37.168512,true),
		(23,'Филаретовская улица',55.991647,37.18341,true),
		(24,'Панфиловский проспект',55.98415,37.179317,false),
		(25,'Универсам',55.980345,37.184157,true),
		(26,'МИЭТ',55.983396,37.21154,true),
		(27,'Магазин Товары для дома',55.993586,37.214151,true),
		(28,'Магазин Океан',55.998436,37.211199,true),
		(29,'Кинотеатр Электрон',56.003604,37.20712,true),
		(30,'1-й торговый центр',56.007077,37.202861,true),
		(31,'Северная',56.014166,37.189626,true);
CALL define_trip(111,ARRAY[3,25,26,27,28,29,30,31]);
CALL define_trip(110,ARRAY[10,16,14,17,18,19,4,21,22,23,24,3]);

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(32,'АЗС',56.008792,37.166178,true),
		(33,'Улица Конструктора Лукина',56.012312,37.156165,true),
		(34,'АЗС',56.008658,37.165097,false),
		(37,'Платформа Алабушево',56.010207,37.140695,false);
CALL define_trip(301,ARRAY[3,25,26,27,28,29,30,32,33,8]);
CALL define_trip(300,ARRAY[8,37,33,34,16,14,17,18,19,4,3]);

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(38,'Западная',56.011655,37.158476,false),
		(39,'Платформа Алабушево',56.007889,37.142674,true);
CALL define_trip(291,ARRAY[3,21,22,15,16,14]);
CALL define_trip(290,ARRAY[14,17,18,19,4,3]);

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(40,'7-й торговый центр',55.987623,37.231666,true),
		(41,'Поликлиника',55.984875,37.238245,true),
		(42,'Поликлиника',55.985863,37.23702,false),
		(43,'7-й торговый центр',55.987683,37.232634,false);
CALL define_trip(201,ARRAY[3,25,26,40,41,6]);
CALL define_trip(200,ARRAY[6,42,43,19,4,3]);

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(44,'Городской пруд',55.984357,37.2204,true),
		(45,'Городской пруд',55.984667,37.219514,false);
CALL define_trip(311,ARRAY[3,25,26,44,7]);
CALL define_trip(310,ARRAY[7,42,43,45,19,4,3]);

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(46,'Студенческая',55.997371,37.224292,true),
		(47,'Студенческая',55.998179,37.222701,false),
		(48,'Московский проспект',55.997058,37.238009,true),
		(49,'Московский проспект',55.997037,37.238509,false),
		(50,'12-й микрорайон',55.994607,37.188679,false);
CALL define_trip(121,ARRAY[3,2,15,16,14,46,48,5]);
CALL define_trip(120,ARRAY[5,49,47,29,30,50,24,3]);

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(51,'Филаретовская улица',55.991867,37.182396,false);
CALL define_trip(901,ARRAY[3,25,26,27,28,29,30,1]);
CALL define_trip(900,ARRAY[1,10,50,51,20,9,3]);

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(53,'14-й микрорайон',55.987549,37.145075,true),
		(54,'Дворец единоборств',55.983878,37.15347,false),
		(55,'Дворец единоборств',55.984235,37.152191,true),
		(56,'Корпус № 1538',55.978141,37.161739,true),
		(57,'Корпус № 1538',55.977705,37.162239,false),
		(58,'МЦД Зеленоград-Крюково',55.980131,37.173,false),
		(59,'МЦД Зеленоград-Крюково',55.980321,37.172734,true);
CALL define_trip(191,ARRAY[53,54,56,58,25,26,27,28,46,7]);
CALL define_trip(190,ARRAY[7,42,43,47,17,18,19,4,59,57,55,11]);

INSERT INTO stops(stop_id,stop_name,latitude,longitude,dir)
VALUES	(60,'Оранжерея',55.979704,37.260605,true),
		(61,'Река Сходня',55.974159,37.262497,false),
		(62,'МЦД Фирсановская',55.96069,37.252091,true),
		(64,'Река Сходня',55.974759,37.262964,true),
		(65,'Оранжерея',55.980114,37.259336,false);
CALL define_trip(271,ARRAY[8,37,32,38,34,16,14,46,40,41,60,61,62]);
CALL define_trip(270,ARRAY[13,64,65,42,43,47,29,30,32,38,34,39,8]);

INSERT INTO drivers(license_id,first_name,last_name,patronymic,date_of_birth)
VALUES	(7701221822,'Дмитрий','Петров','Владиславович','25-07-2000'),
		(7701254688,'Арсен','Жуманбинов',null,'01-03-1994'),
		(4427897987,'Аркадий','Паровозов','Иванович','08-09-1985'),
		(8718727910,'Иван','Евдокимов','Сергеевич','15-12-1997'),
		(7289871979,'Григорий','Лейкин','Артёмович','22-06-2001'),
		(7701217981,'Василий','Копытов','Романович','11-11-1996'),
		(7701145678,'Фёдор','Крылкин','Евгеньевич','14-10-1995'),
		(1465168210,'Никита','Осинный','Никитович','05-08-1999'),
		(7701001860,'Илья','Апельсинов','Кириллович','04-02-1998'),
		(7701271992,'Захар','Забегайкин','Александрович','17-06-2001');
UPDATE drivers SET work_hours=random()*140;

INSERT INTO trips_departures(trip_id,departure_time,license_id,weekend)
VALUES	(801,'10:08',7701254688,false),(800,'11:20',7701217981,false),
		(291,'15:30',7701271992,true),(290,'16:50',7701271992,true),
		(121,'20:08',7701217981,false),(120,'23:10',7701254688,false),
		(201,'15:01',7701254688,true),(200,'12:13',null,true),
		(311,'14:56',7701221822,true),(310,'16:03',1465168210,false),
		(301,'22:00',7701221822,false),(300,'17:04',7701145678,false),
		(901,'19:15',7701001860,true),(900,'11:30',1465168210,false),
		(401,'07:50',7289871979,false),(400,'09:04',7701001860,false),
		(271,'11:48',7701217981,true),(270,'08:46',7701145678,false),
		(111,'18:45',7289871979,false),(110,'13:16',7701254688,false),
		(191,'17:23',1465168210,true),(190,'13:17',7701145678,false);

INSERT INTO trips_departures(trip_id,departure_time,license_id,weekend)
VALUES	(800,'11:01',7701145678,false);