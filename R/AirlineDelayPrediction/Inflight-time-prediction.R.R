#
# This is a Shiny web application for determining if in flight time can
# make up for arrival delays to minimize destination delays.
# Year taken is 2008 for sample run.
# You can run the application by clicking the 'Run App' button in R-Studio
# For the application to launch, takes close to 5 minutes on a 8 GB RAM machine.
# The app uses a linear regression model to calculate the inflight make-up time.
# Further work will be to host the R Shiny app from local server to a public hosted domain.

## References
#1 Installing apache spark for R-Studio 
    ## <https://spark.rstudio.com/>
#2 predicting airline delays using SparkR 
    ## <https://hortonworks.com/tutorial/predicting-airline-delays-using-sparkr/>


library(dplyr)
library(ggplot2)
library(DT)
library(leaflet)
library(geosphere)


# Read in the flights and the airlines data set for 2008.
flights_2008 <-  read.csv("flights-2008.csv")
airlines_2008 <- read.csv("airlines-2008.csv")
airports_2008 <- read.csv("airports-2008.csv")


## Set your installed Java path here
java_path <- normalizePath('C:/Program Files (x86)/Java/jre1.8.0_144')
Sys.setenv(JAVA_HOME=java_path)
library(sparklyr)
sc <- spark_connect(master = "local")

print('Apache spark local connection is successful')

copy_to(sc, flights_2008, "flights_s", overwrite = TRUE)
flights_tbl <- tbl(sc, 'flights_s')

copy_to(sc, airlines_2008, "airlines_s", overwrite = TRUE)
airlines_tbl <- tbl(sc, 'airlines_s')

model_data <- flights_tbl %>%
    filter(!is.na(arr_delay) & !is.na(dep_delay) & !is.na(distance)) %>%
    filter(dep_delay > 15 & dep_delay < 240) %>%
    filter(arr_delay > -60 & arr_delay < 360) %>%
    left_join(airlines_tbl, by = c("carrier" = "carrier")) %>%
    mutate(gain = dep_delay - arr_delay) %>%
    select(origin, dest, carrier, airline = name, distance, dep_delay, arr_delay, gain)

partitions <- model_data %>%
    sdf_partition(train_data = 0.5, valid_data = 0.5, seed = 777)

lm1 <- ml_linear_regression(partitions$train_data, gain ~ distance + dep_delay + carrier)

pred_tbl <- sdf_predict(lm1, partitions$valid_data)

lookup_tbl <- pred_tbl %>%
    group_by(origin, dest, carrier, airline) %>%
    summarize(
        flights = n(),
        distance = mean(distance),
        avg_dep_delay = mean(dep_delay),
        avg_arr_delay = mean(arr_delay),
        avg_gain = mean(gain),
        pred_gain = mean(prediction)
    )

sdf_register(lookup_tbl, "lookup")
tbl_cache(sc, "lookup")

carrier_origin <- c("JFK", "LGA", "EWR")
carrier_dest <- c("BOS", "DCA", "DEN", "HNL", "LAX", "SEA", "SFO", "STL")

ui <- fluidPage(
    
    tags$script(' var setInitialCodePosition = function() 
                { setCodePosition(false, false); }; '),
    
    titlePanel("Time Gained in Flight for flights departing in 2008 for selected airports"),
    
    sidebarLayout(
        sidebarPanel(
            radioButtons("origin", "Flight origin:",
                         carrier_origin, selected = "JFK"),
            br(),
            
            radioButtons("dest", "Flight destination:",
                         carrier_dest, selected = "SFO")
            
        ),
        
        mainPanel(
            tabsetPanel(type = "tabs", 
                        tabPanel("Plot", plotOutput("plot")), 
                        tabPanel("Data", dataTableOutput("datatable"))
            )
        )
    )
    )

# Shiny server function
server <- function(input, output) {
    
    origin <- reactive({
        req(input$origin)
        filter(airports_2008, faa == input$origin)
    })
    
    dest <- reactive({
        req(input$dest)
        filter(airports_2008, faa == input$dest)
    })
    
    plot_data <- reactive({
        req(input$origin, input$dest)
        lookup_tbl %>%
            filter(origin==input$origin & dest==input$dest) %>%
            ungroup() %>%
            select(airline, flights, distance, avg_gain, pred_gain) %>%
            collect
    })
    
    output$plot <- renderPlot({
        ggplot(plot_data(), aes(factor(airline), pred_gain)) + 
            geom_bar(stat = "identity", fill = '#2780E3') +
            geom_point(aes(factor(airline), avg_gain)) +
            coord_flip() +
            labs(x = "", y = "Time gained in flight (minutes)") +
            labs(title = "Observed gain (point) vs Predicted gain (bar)")
    })
    
    output$datatable <- renderDataTable(
        datatable(plot_data()) %>%
            formatRound(c("flights", "distance"), 0) %>%
            formatRound(c("avg_gain", "pred_gain"), 1)
    )
    
}

shinyApp(ui = ui, server = server)