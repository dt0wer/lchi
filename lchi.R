library(rvest, quietly = T)
library(httr, quietly = T)
library(data.table, quietly = T)
library(dtplyr, quietly = T)
library(dplyr, quietly = T)

find_last_page <- function() {
  go_next <- TRUE
  next_page <- 1
  
  while(go_next) {
    doc <- read_lchi_page(next_page)
    pnav <- html_nodes(doc, css = '.pnav')
    
    next_page <- gsub("^.+fChPage\\(([0-9]+)\\).+$", "\\1", 
                      pnav[grepl(pattern = "fChPage", pnav)], perl = T) %>% 
      as.integer() %>% unique() %>% max()
    
    go_next <- any(grepl("След", html_text(pnav)))
  }
  
  next_page
}

read_lchi_page <- function(x) {
  url <- "http://investor.moex.com/ru/statistics/2017/default.aspx"
  
  resp <- POST(url, body=list(act = '',
                              sby = 8,
                              nick = '',
                              `data-contype-id-1` = '',
                              `data-contype-id-2` = '',
                              `data-contype-id-3` = '',
                              pge = x), encode="form")
  content(resp)
}

parse_lchi_userlist <- function(page) {
  html_nodes(read_lchi_page(page), css = '.nickname') %>% 
    html_nodes("a") %>% html_attr("href")
}

read_all_ids <- function() {
  as.list(do.call("c", lapply(seq(1:find_last_page()), parse_lchi_userlist)))
}

generate_js <- function(x) {
  js <- paste0("var webPage = require('webpage');
               var page = webPage.create();
               
               var fs = require('fs');
               var path = 'userpage.html'
              
               page.settings.loadImages = false
               
               page.open('http://investor.moex.com", x,"', function (status) {
               var content = page.content;
               fs.write(path,content,'w')
               phantom.exit();
               });")
  f <- file("scrape_lchi.js", "w")
  writeLines(js, con = f)
  close(f)
}

read_user_data <- function(url) {
  generate_js(url)
  system("./phantomjs scrape_lchi.js", show.output.on.console = F)

  html_file <- read_html("userpage.html")
  
  spinner <- html_nodes(html_file, xpath='//span[@spinner-key="spinner-6"]') %>% 
    xml_children()
  
  if(length(spinner) == 0) {
    deals <- html_nodes(html_file, xpath='//div[@class="for_table"]/table') %>% 
      html_table()
    
    deals <- deals[[length(deals)]]
    
    colnames(deals) <- c("MARKET", "SEC", "POSITION", "PRICE", "BALANCE", 
                         "COST")
    as_tibble(deals) %>% select(SEC, POSITION) %>%
      mutate_at("POSITION", function(x) gsub("^(-?[0-9]\\d*).+$", "\\1", x)) %>% 
        mutate_at("POSITION", as.numeric)
  } else NULL
}

read_all_users <- function() {
  raw <- read_n_users()
  mutate(raw, LONG = ifelse(POSITION > 0, POSITION, 0), 
         SHORT = ifelse(POSITION < 0, POSITION, 0)) %>% group_by(SEC) %>% 
    summarize(LONG = sum(LONG), SHORT = sum(SHORT))
}

read_n_users <- function(ids = NULL, n = NULL) {
  datalist = list()
  
  if (is.null(ids)) ids <- read_all_ids()
  if (is.null(n)) n <- length(ids)

  ids <- ids[1:n]
  
  idx <- 0
  while(idx < n) {
    dat <- read_user_data(ids[1])
    if (is.null(dat)) {
      ids[length(ids) + 1] <- ids[1]
    } else {
      idx <- idx + 1
      print(paste0(idx, "/", n, ": ", ids[1]))
      datalist[[idx]] <- dat
    }
    ids[1] <- NULL
  }
  
  do.call(bind_rows, datalist)
}