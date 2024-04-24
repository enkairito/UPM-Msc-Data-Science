# Packages
library(mapproj)
library(shiny)
library(ggplot2)
library(ggstream)
library(tidyverse)
library(plotly)

#Load Data
data <- data.frame(read.csv("Data/ds_salaries.csv"))

# User Interface
userInterface <- fluidPage(
  titlePanel("💵 Data Science Salaries Analysis 💵"),
  
# Navigation Bar
  navbarPage("",
             tabPanel("🗺 Map Chart️", 
                      fluidRow(
                        style ="background-color: #F4F5F5;padding: 5px;",
                        titlePanel("Map Chart 🗺"),
                        helpText("How do salaries vary depending on the company location or employee residence in the world?"),
                        column(5, selectInput("var", 
                                              label = "Variable:",
                                              choices = c("Company Location", "Employee Residence"),
                                              selected = "Company Location")),
                        column(5, sliderInput("range", 
                                              label = "Range of Salary in Dollars:",
                                              min = 5132, max = 450000, value = c(5132, 450000), pre='$')),
                        column(12,plotOutput("map"))
                      )
             ),
             tabPanel("📊 Bar Chart", 
                      fluidRow(
                        style ="background-color: #F4F5F5;padding: 5px;",
                        titlePanel("Bar Chart 📊"),
                        helpText("How does salary interact with different variables?"),
                        column(10,
                               selectInput("chart", 
                                           label = "X variable:",
                                           choices = c("Company Size", 
                                                       "Employment Type",
                                                       "Remote Ratio",
                                                       "Experience Level"),
                                           selected = "Company Size")
                      ),
                      column(10,plotlyOutput("barChart")))
             ),
             tabPanel("🌊 Stream Graph", 
                      fluidRow(
                        style ="background-color: #F4F5F5;padding: 5px;",
                        titlePanel("Stream Graph 🌊"),
                        helpText("How mean salary and job opportunities have fluctuated for each type of company and experience level per year?"),
                        column(5,
                               selectInput("option", 
                                           label = "Y variable:",
                                           choices = c("Mean Salary", 
                                                       "Job Oportunities"),
                                           selected = "Mean Salary")),
                        column(5,
                               selectInput("type", 
                                           label = "Stream Graph Visualization Types:",
                                           choices = c("Ridge", 
                                                       "Proportional"),
                                           selected = "Ridge")
                        ),
                        plotOutput("streamGraph"))
             )
    )
)




# Main
main <- function(input, output) {
  
  ##--------------------------------------## 
  ##----------------##Map##---------------##
  ##--------------------------------------##
  output$map <- renderPlot({
    data <- switch(input$var, 
                   "Company Location" = countries_company,
                   "Employee Residence" = countries_employee)
    
    color <- switch(input$var, 
                    "Company Location" = "darkorange",
                    "Employee Residence" = "darkgreen")
    
    legend <- switch(input$var, 
                     "Company Location" = "Company Location Salaries",
                     "Employee Residence" = "Employee Residence Salaries")

    mapper(data, color, legend, input$range[1], input$range[2])
  })
  
  ##--------------------------------------## 
  ##-------------##Bar Chart##------------##
  ##--------------------------------------##
  output$barChart <- renderPlotly({
    
    columnChoose <- switch(input$chart, 
                           "Company Size" = "company_size",
                           "Experience Level" = "experience_level",
                           "Employment Type" = "employment_type",
                           "Remote Ratio" = "remote_ratio",)
    
    plot <- plot_ly(x = ~data[[columnChoose]],  y = ~data$salary_in_usd
                    , type = "violin"
                    , box = list(visible = TRUE))
    
    plot <- plot %>% layout(title = paste("Comparing Salary in $ by", input$chart ),
                            xaxis = list(title = input$chart),
                            yaxis = list(title = "Salary in $")) 
    
    plot
  })
  
  ##--------------------------------------## 
  ##-----------##Stream Graph##-----------##
  ##--------------------------------------##
  output$streamGraph <- renderPlot({
    if (input$option == "Mean Salary") {
      # Subset with mean salary for each work_year and company_size
      subset_data <- data %>%
        group_by(work_year, company_size) %>%
        summarise(MeanSalaryUSD = mean(salary_in_usd))
      if(input$type == "Ridge") {
        ggplot(subset_data, aes(x = work_year, y = MeanSalaryUSD, fill = company_size)) +
          geom_stream(type = "ridge", bw=1.2)+ labs( x = "Year", y= "Mean Salary in USD")  + 
          guides(fill = guide_legend(title = "Company Size")) + scale_y_continuous(labels = scales::comma_format())

      }
      else{
        ggplot(subset_data, aes(x = work_year, y = MeanSalaryUSD, fill = company_size)) +
          geom_stream(type = "proportional", bw=1.2)  + labs( x = "Year", y= "Mean Salary in USD") + 
          guides(fill = guide_legend(title = "Company Size"))+ scale_y_continuous(labels = scales::comma_format())
      }
    }
    
    else if( input$option == "Job Oportunities" ){
      # Subset with job opportunities for each work_year and company_size
      subset_data <- data %>%
        group_by(work_year, company_size) %>%
        summarise(NumberOfJobs = n())
      if(input$type == "Ridge") {
        ggplot(subset_data, aes(x = work_year, y = NumberOfJobs, fill = company_size)) +
          geom_stream(type = "ridge", bw=1.15)+ labs( x = "Year", y= "Number of Jobs")  + 
          guides(fill = guide_legend(title = "Company Size"))+ scale_y_continuous(labels = scales::comma_format())
      }
      else{
        ggplot(subset_data, aes(x = work_year, y = NumberOfJobs, fill = company_size)) +
          geom_stream(type = "proportional", bw=1.15)  +labs( x = "Year", y= "Number of Jobs") + 
          guides(fill = guide_legend(title = "Company Size"))+ scale_y_continuous(labels = scales::comma_format())
      }
    }
  })
}


##------------------------------------------------------------------------------------------------------------------##
##-------------------------------------------------##HELPER METHOD##------------------------------------------------##
##------------------------------------------------------------------------------------------------------------------##
##------------------------------------------------------------------------------------------------------------------##
mapping_company_location <- c(
  "ES" = "Spain", "US" = "USA", "CA" = "Canada", "DE" = "Germany", "GB" = "UK:Great Britain",
  "NG" = "Nigeria", "IN" = "India", "HK" = "Hong Kong", "NL" = "Netherlands", "CH" = "Switzerland",
  "CF" = "Central African Republic", "FR" = "France", "FI" = "Finland", "UA" = "Ukraine", "IE" = "Ireland",
  "IL" = "Israel", "GH" = "Ghana", "CO" = "Colombia", "SG" = "Singapore", "AU" = "Australia", "SE" = "Sweden",
  "SI" = "Slovenia", "MX" = "Mexico", "BR" = "Brazil", "PT" = "Portugal", "RU" = "Russia", "TH" = "Thailand",
  "HR" = "Croatia", "VN" = "Vietnam", "EE" = "Estonia", "AM" = "Armenia", "BA" = "Bosnia and Herzegovina",
  "KE" = "Kenya", "GR" = "Greece", "MK" = "North Macedonia", "LV" = "Latvia", "RO" = "Romania", "PK" = "Pakistan",
  "IT" = "Italy", "MA" = "Morocco", "PL" = "Poland", "AL" = "Albania", "AR" = "Argentina", "LT" = "Lithuania",
  "AS" = "American Samoa", "CR" = "Costa Rica", "IR" = "Iran", "BS" = "The Bahamas", "HU" = "Hungary", "AT" = "Austria",
  "SK" = "Slovakia", "CZ" = "Czech Republic", "TR" = "Turkey", "PR" = "Puerto Rico", "DK" = "Denmark", "BO" = "Bolivia",
  "PH" = "Philippines", "BE" = "Belgium", "ID" = "Indonesia", "EG" = "Egypt", "AE" = "United Arab Emirates", "LU" = "Luxembourg",
  "MY" = "Malaysia", "HN" = "Honduras", "JP" = "Japan", "DZ" = "Algeria", "IQ" = "Iraq", "CN" = "China", "NZ" = "New Zealand",
  "CL" = "Chile", "MD" = "Moldova", "MT" = "Malta"
)

mapping_employee_residence <- c(
  "ES" = "Spain", "US" = "USA", "CA" = "Canada", "DE" = "Germany", "GB" = "UK:Great Britain",
  "NG" = "Nigeria", "IN" = "India", "HK" = "Hong Kong", "PT" = "Portugal", "NL" = "Netherlands",
  "CH" = "Switzerland", "CF" = "Central African Republic", "FR" = "France", "AU" = "Australia",
  "FI" = "Finland", "UA" = "Ukraine", "IE" = "Ireland", "IL" = "Israel", "GH" = "Ghana",
  "AT" = "Austria", "CO" = "Colombia", "SG" = "Singapore", "SE" = "Sweden", "SI" = "Slovenia",
  "MX" = "Mexico", "UZ" = "Uzbekistan", "BR" = "Brazil", "TH" = "Thailand", "HR" = "Croatia",
  "PL" = "Poland", "KW" = "Kuwait", "VN" = "Vietnam", "CY" = "Cyprus", "AR" = "Argentina",
  "AM" = "Armenia", "BA" = "Bosnia and Herzegovina", "KE" = "Kenya", "GR" = "Greece", "MK" = "North Macedonia",
  "LV" = "Latvia", "RO" = "Romania", "PK" = "Pakistan", "IT" = "Italy", "MA" = "Morocco", "LT" = "Lithuania",
  "BE" = "Belgium", "AS" = "American Samoa", "IR" = "Iran", "HU" = "Hungary", "SK" = "Slovakia",
  "CN" = "China", "CZ" = "Czech Republic", "CR" = "Costa Rica", "TR" = "Turkey", "CL" = "Chile",
  "PR" = "Puerto Rico", "DK" = "Denmark", "BO" = "Bolivia", "PH" = "Philippines", "DO" = "Dominican Republic",
  "EG" = "Egypt", "ID" = "Indonesia", "AE" = "United Arab Emirates", "MY" = "Malaysia", "JP" = "Japan",
  "EE" = "Estonia", "HN" = "Honduras", "TN" = "Tunisia", "RU" = "Russia", "DZ" = "Algeria", "IQ" = "Iraq",
  "BG" = "Bulgaria", "JE" = "Jersey", "RS" = "Serbia", "NZ" = "New Zealand", "MD" = "Moldova",
  "LU" = "Luxembourg", "MT" = "Malta"
)

data$company_location_mapped <- mapping_company_location[data$company_location]
data$employee_residence_mapped <- mapping_employee_residence[data$employee_residence]
countries_company <- unique(data$company_location_mapped)
countries_employee <- unique(data$employee_residence_mapped)
salary <- data$salary_in_usd

mapper <- function(countries, color, legend.title, min=5132, max=450000) {
  print("Countries:")
  print(countries)
  #Generate vector of fill colors for map
  shades <- colorRampPalette(c("white", color))(100)
  
  #Constrain gradient to percents that occur between min and max
  salary <- pmax(salary, min)
  salary <- pmin(salary, max)
  
  
  percents <- as.integer(cut(salary, 100, 
                             include.lowest = TRUE, ordered = TRUE))
  fills <- shades[percents]

  #Plot map
  maps::map("world",col = fills, regions = countries, fill = TRUE, projection="mercator", wrap = c(-180,180, NA))
  
  #Borders
  maps::map("world", col = "black", fill = FALSE, add = TRUE, projection="mercator",  wrap = c(-180,180, NA))
  
  #Legend
  inc <- (max - min) / 4
  legend.text <- c(paste0(min, " or less $"),
                   paste0(min + inc, " $"),
                   paste0(min + 2 * inc, " $"),
                   paste0(min + 3 * inc, " $"),
                   paste0(max, " or more $"))
  
  legend("bottomlef", 
         legend = legend.text, 
         fill = shades[c(1, 25, 50, 75, 100)], 
         title = legend.title)
}

##------------------------------------------------------------------------------------------------------------------##
##------------------------------------------------------------------------------------------------------------------##
##------------------------------------------------------------------------------------------------------------------##
##------------------------------------------------------------------------------------------------------------------##

# Run App
shinyApp(userInterface, main)
