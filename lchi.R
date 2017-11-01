library(httr)
library(XML)
library(data.table)
library(dtplyr)
library(dplyr)

find_last_page <- function() {
  go_next <- TRUE

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
  doc <- content(resp)  
}

parse_lchi_userlist <- function(page) {
  html_nodes(read_lchi_page(page), css = '.nickname') %>% 
    html_nodes("a") %>% html_attr("href")
}

read_all_ids <- function() {
  do.call("c", lapply(seq(1:find_last_page()), parse_lchi_userlist))
}

generate_js <- function(x) {
  js <- paste0("var webPage = require('webpage');
var page = webPage.create();

var fs = require('fs');
var path = 'userpage.html'

page.open('http://investor.moex.com", x,"', function (status) {
  var content = page.content;
  fs.write(path,content,'w')
  phantom.exit();
});")
  f <- file("scrape_lchi.js", "w")
  writeLines(js, con = f)
  close(f)
}

read_user_data_table <- function(url) {
  system("./phantomjs scrape_lchi.js", show.output.on.console = F)
  df <- readHTMLTable(htmlParse("userpage.html"), stringsAsFactors = F)
  deals <- df[[length(df)]]
  colnames(deals) <- c("MARKET", "SEC", "POSITION", "PRICE", "BALANCE", "COST")
  v <- vars(POSITION, PRICE, BALANCE, COST)
  as_tibble(deals) %>% mutate_at("POSITION", 
                                 function(x) gsub("(\\d+).+", "\\1", x)) %>% 
    mutate_at(v, function(x) gsub("-", "0", x)) %>% 
    mutate_at(v, as.numeric)
}
