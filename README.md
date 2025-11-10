All env variables are stored in Admin-> Variables in Airflow.                                                        
All 100 countryes are stored in Admin-> Variables in Airflow as json.                                                            
Api key and data base crediantials are stored in  Admin-> Variables in Airflow                                                   
                                                                                                                  
fetch_weather_data()                                                                                                 
it fetches weather data of 100 countryes and store in "weather_data" tabel in postgress                                                  
data_hash()- is used to save time and increas efficiency                                                                         
                                                                                                                                
transform_weather_data()                                                                   
It takes data from "weather_data"tabel and do transfromations and store back in "final_res"tabel                                                                       
                                                                                                                              
DAG                                                                                                                      
it perform hourly Airflow orchestration                                                                    
                                                                                                                        
                                                                                                                                    
Airflow worksperfectly                                                                                                              
<img width="733" height="381" alt="image" src="https://github.com/user-attachments/assets/f8cf767c-95cc-42db-8d04-b8d44637fa4b" />
                                                                                                                                  
                                                                                                                                    
final_res tabel
<img width="1885" height="759" alt="image" src="https://github.com/user-attachments/assets/0262388f-3496-45c1-a5f7-3f23afa78f0f" />
                                                                                                                                                                                                                                                                                
