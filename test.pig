--Autor Aida
REGISTER '/home/ubuntu/lib/piggybank.jar';
data = LOAD '/home/ubuntu/data/ofcom-postcode-data-for-consumers-2013.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS
(Postcode:chararray, postcode_data_status:chararray, Lines_less_2Mbps:chararray, average_speed_Mbps:chararray, Median_Speed_Mbps:chararray, Maximum_Speed_Kbps:chararray, NGA_Available:chararray, Number_of_Connections:int);

fdata = filter data by average_speed_Mbps IS NOT NULL AND Maximum_Speed_Kbps IS NOT NULL AND Maximum_Speed_Kbps != 'N/A';
/*
sort_mx_speed = ORDER fdata BY Maximum_Speed_Kbps DESC;

grouped_speed = group sort_mx_speed by Maximum_Speed_Kbps;

freq_counts = FOREACH grouped_speed GENERATE
  group as grouped_speed,  -- the key you grouped on
  COUNT(sort_mx_speed),
  flatten(group); -- the number of tuple with this speed)

ordered_freq_counts = ORDER freq_counts BY group DESC;
ldata = limit ordered_freq_counts 5;

--rmf /home/ubuntu/aida/output-1
--STORE ldata INTO '/home/ubuntu/aida/output-1' USING PigStorage(',');
*/

top_max_speed = FILTER fdata BY Maximum_Speed_Kbps IN ('>=30', '9.9', '9.8', '9.7', '9.6');
max_speed = GROUP top_max_speed BY (Maximum_Speed_Kbps);

B = FOREACH max_speed {
  sorted = ORDER top_max_speed by average_speed_Mbps DESC;
  lim = LIMIT sorted 5;
  GENERATE lim;
};
describe B;
--({(SY162NR,OK,N,9.6,9.6,9.6,N,3),(YO111QP,OK,N,9.6,9.6,9.6,Y,3),(B742RB,OK,N,9.6,9.6,9.6,N,3),(CA12PU,OK,N,9.2,9.1,9.6,N,4),(DT28HH,OK,N,9.2,9.2,9.6,N,4)})
--({(CM159SR,OK,N,9.7,9.7,9.7,N,3),(DH13DS,OK,N,9.7,9.7,9.7,Y,4),(G59ST,OK,N,9.7,9.7,9.7,N,3),(WD61JN,OK,N,9.7,9.7,9.7,Y,3),(N16DX,OK,N,9.7,9.7,9.7,Y,3)})
--({(L82US,OK,N,9.8,9.8,9.8,Y,3),(KT172NE,OK,N,9.6,9.6,9.8,Y,3),(BD40NU,OK,N,9.6,9.6,9.8,Y,3),(BD215HA,OK,N,9.6,9.6,9.8,Y,3),(SE42NT,OK,N,9.6,9.7,9.8,N,3)})
--({(CW13LS,OK,N,9.8,9.8,9.9,Y,4),(DG112FF,OK,N,9.8,9.8,9.9,N,3),(KY129EY,OK,N,9.8,9.8,9.9,Y,3),(HP109LR,OK,N,9.5,9.9,9.9,Y,3),(N77NN,OK,N,9.5,9.4,9.9,Y,3)})
--({(PE14RD,OK,N,>=30,25,>=30,Y,12),(PA59JF,OK,N,>=30,25,>=30,Y,19),(PE14PZ,OK,Y,>=30,>=30,>=30,Y,22),(PE14PY,OK,Y,>=30,>=30,>=30,Y,14),(PA59JX,OK,Y,>=30,20,>=30,Y,14)})


top5 = FOREACH B GENERATE lim.Maximum_Speed_Kbps,lim.Postcode, lim.average_speed_Mbps;
dump top5;
--({(9.6),(9.6),(9.6),(9.6),(9.6)},{(SY162NR),(YO111QP),(B742RB),(CA12PU),(DT28HH)},{(9.6),(9.6),(9.6),(9.2),(9.2)})
--({(9.7),(9.7),(9.7),(9.7),(9.7)},{(CM159SR),(DH13DS),(G59ST),(WD61JN),(N16DX)},{(9.7),(9.7),(9.7),(9.7),(9.7)})
--({(9.8),(9.8),(9.8),(9.8),(9.8)},{(L82US),(KT172NE),(BD40NU),(BD215HA),(SE42NT)},{(9.8),(9.6),(9.6),(9.6),(9.6)})
--({(9.9),(9.9),(9.9),(9.9),(9.9)},{(CW13LS),(DG112FF),(KY129EY),(HP109LR),(N77NN)},{(9.8),(9.8),(9.8),(9.5),(9.5)})
--({(>=30),(>=30),(>=30),(>=30),(>=30)},{(PE14RD),(PA59JF),(PE14PZ),(PE14PY),(PA59JX)},{(>=30),(>=30),(>=30),(>=30),(>=30)})




