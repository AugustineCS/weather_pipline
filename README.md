env variables are stored in Admin-> Variables in Airflow.                                                        
All 100 countryes are stored in Admin-> Variables in Airflow as json.                                                            
Api key and data base crediantials are stored in  Admin-> Variables in Airflow                                                   
                                                                                                                  
fetch_weather_data()                                                                                                 
it fetches weather data of 100 countryes and store in "weather_data" tabel in postgress                                                  
data_hash()- is used to save time and increas efficiency                                                                         
                                                                                                                                
transform_weather_data()                                                                   
It takes data from "weather_data"tabel and do transfromations and store back in "final_res"tabel                                                                       
                                                                                                                              
DAG                                                                                                                      
it perform hourly Airflow orchestration                                                                    

