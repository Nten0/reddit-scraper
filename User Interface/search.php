<?php
session_start();
if($_SERVER["REQUEST_METHOD"] == "POST") {
  $servername = "localhost";
  $username = "root";
  $password = "root";
  $db = "Project_LP";
  $input = $_POST['input'];
  $i = 0;
  $output = "";
  $token = strtok($input," ");
  while($token !== false){
    $tokens[$i] = $token;
    $output = $output . "/" . $tokens[$i]; 
    $i = $i + 1;
    $token = strtok(" ");
  }
  $conn = mysqli_connect($servername, $username, $password, $db);
  if (!$conn)
      die("Connection failed: " . mysqli_connect_error());
  $utf8 = "SET character_set_results = 'utf8', character_set_client = 'utf8', character_set_connection = 'utf8', character_set_database = 'utf8', character_set_server = 'utf8'";
  mysqli_query($conn,$utf8);
  for ($j = 0; $j < $i; $j++)
  {
       $sql = "SELECT doc_id,weight FROM InvertedDocs WHERE ID IN (SELECT invID FROM InvertedIndex WHERE Word LIKE '%$tokens[$j]%') ORDER BY weight DESC, doc_id";
      $k = 0;
      $result = mysqli_query($conn,$sql);
       
      while($row = $result->fetch_assoc())
      {
        if ($j ==0)
        { 
          if($k == 0)
          {
            $results[$row["doc_id"]]= $row['weight'];        
            $k++;
          }
          else
          {
            $flag = 0;
            foreach ($results as $key => $value) 
            {
              if(strcmp($key, $row["doc_id"]) == 0 )  
                $flag = 1;
            }
            if($flag == 0)
                  $results[$row["doc_id"]]= $row['weight'];     
          }
        }
        else
        {
          if($k == 0)
          {
            $results[$row["doc_id"]]+= $row['weight'];        
            $k++;
          }
          else
          {
            $flag = 0;
            foreach ($results as $key => $value) 
            {
              if(strcmp($key, $row["doc_id"]) == 0 )
                $flag = 1;
            }
            if($flag == 0)
                $results[$row["doc_id"]]+= $row['weight'];        
          }
        }
      }
    }
    arsort($results);
    $articles = array();
    foreach ($results as $key => $value)
    {
      $sql = "SELECT ID,URL,Title FROM Articles WHERE ID = $key"; 
      $result = mysqli_query($conn,$sql);
      if ($result->num_rows > 0) {

        while($row = $result->fetch_assoc())
        {
            array_push($articles,[
              'id' =>$row['ID'],
              'url' => $row['URL'],
              'title' =>$row["Title"],
              'score' => $value
            ]);
        }
      } 
      
    }
    $someJSON=json_encode($articles);
    echo $someJSON;
}
?>


