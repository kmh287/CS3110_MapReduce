
open WordCount
open InvertedIndex
open DnaSequencing

let list_apps () =
  let apps = String.concat "\n  - " (MapReduce.list_apps ()) in
  if apps = "" then "No apps installed!" else "Installed apps:\n\n  - "^apps

