# Should_I_Drive

## **Motivation**

Living in the big city you have multiple choices to travel around, however, I believe most of us still prefer to drive. The million dollar question you always ask is "Could I find a parking meter spot?" If you are like me, who wanted a peaceful mind, would appreciate some risk assesment!
![Image of motivation](https://github.com/wanlindu/Should_I_Drive/blob/master/image/mot.jpg)

## **Solution**
What about some historical data analysis? If you could give me your destination, your time to arrive and the range you would like to walk, I will do the digging work and find historically what is chance that you could find a parking spot!
![Image of solution](https://github.com/wanlindu/Should_I_Drive/blob/master/image/sol.jpg)

## **Architecture**
Parking Events data are stored in SF public dataset and updated once a week. Data need to be clean up and join with parking meter base to add actual locations before written into database, as it will be updated every week, airflow is employed to repeat this work. Once data is cleaned and prepared, users could interact with data through Dash.
![Image of tech deck](https://github.com/wanlindu/Should_I_Drive/blob/master/image/pipline.jpg)
