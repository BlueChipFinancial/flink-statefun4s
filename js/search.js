// When the user clicks on the search box, we want to toggle the search dropdown
function displayToggleSearch(e) {
  e.preventDefault();
  e.stopPropagation();

  closeDropdownSearch(e);
  
  if (idx === null) {
    console.log("Building search index...");
    prepareIdxAndDocMap();
    console.log("Search index built.");
  }
  const dropdown = document.querySelector("#search-dropdown-content");
  if (dropdown) {
    if (!dropdown.classList.contains("show")) {
      dropdown.classList.add("show");
    }
    document.addEventListener("click", closeDropdownSearch);
    document.addEventListener("keydown", searchOnKeyDown);
    document.addEventListener("keyup", searchOnKeyUp);
  }
}

//We want to prepare the index only after clicking the search bar
var idx = null
const docMap = new Map()

function prepareIdxAndDocMap() {
  const docs = [  
    {
      "title": "Home",
      "url": "/flink-statefun4s/",
      "content": ""
    } ,    
    {
      "title": "Intro",
      "url": "/flink-statefun4s/docs/",
      "content": "Flink Statefun4s This library provides a Scala SDK for remote Flink Stateful Functions. Flink functions have some state managed by Flink, take and event, and send messages to other functions. Flink manages the complexity of dealing with failures and rollback state during exceptional circumstances. This means that if there is a failure in communication between two functions all function states are rolled back and retried until it succeeds. This protects from issues where systems can get into a partial state that doesn’t rollback properly in failure scenarios."
    } ,        
    {
      "title": "StatefulFunction",
      "url": "/flink-statefun4s/docs/statefulfunction/",
      "content": "StatefulFunction This typeclass provides access to all the functionality provided by Flink Stateful Functions. All the methods it provides are based around managing state and sending messages. Working with State StatefulFunction provides methods around accessing/mutating state. This state is committed in Flink and has ACID, exactly once guarantees. Example import cats.effect._, cats.implicits._ import com.bcf.statefun4s._ import scala.annotation.nowarn case class GreeterRequest(name: String) case class GreeterState(num: Int) def greeter[F[_]: StatefulFunction[*[_], GreeterState]: Sync]( @nowarn input: GreeterRequest ): F[Unit] = { val statefun = StatefulFunction[F, GreeterState] for { newCount &lt;- statefun.insideCtx(_.num + 1) _ &lt;- statefun.modifyCtx(_.copy(newCount)) } yield () } This greeter function accepts a message with a user’s name and tied to the instance of that function is some GreeterState instance. So for example, if a consumer were to send message to example/greeter with the ID of John, it will create a new instance of GreeterState for John and any future messages sent to that function ID will use the current GreeterState for John. So if we sent one message and increment num, then send another message with the same ID, Flink will give us the previous value of num for us to increment it. Since this state is committed in lockstep with Kafka, we can safely increment this number without keeping a unique set of ids and counting them idempotently. Sending Messages Regular messages are sent to another function immediately. If the function ID sent to doesn’t exist, Flink makes a new inbox/state record for that function and passes the message. Example With the following protobuf definitions: message PrinterRequest { string msg = 1; } message GreeterRequest { string name = 1; } message GreeterState { int64 num = 1; } import cats.effect._, cats.implicits._ import com.bcf.statefun4s._ def greeter[F[_]: StatefulFunction[*[_], GreeterState]: Sync]( input: GreeterRequest ): F[Unit] = { val statefun = StatefulFunction[F, GreeterState] for { newCount &lt;- statefun.insideCtx(_.num + 1) _ &lt;- statefun.modifyCtx(_.copy(newCount)) _ &lt;- statefun.sendMsg( \"example\", \"printer\", \"universal\", PrinterRequest(s\"Saw ${input.name} ${newCount} time(s)\") ) } yield () } def printer[F[_]: StatefulFunction[*[_], Unit]: Sync]( input: PrinterRequest ): F[Unit] = Sync[F].delay(println(input.msg)) This will send the output of each counter increment to the printer and print it to stdout. Note that in this case, printer is stateless so we use Unit as the state structure and pass some arbitrary string like \"universal\". Delayed messages Delayed messages are stored in Flink state and scheduled to run at a different point in time. They do not require any events flowing through Flink to trigger. This is very useful for simulating windows or other more complex behavior. Example import cats.effect._, cats.implicits._ import com.bcf.statefun4s._ import com.google.protobuf.any import scala.concurrent.duration._ import java.util.UUID sealed trait Cron object Cron { final case class Create(cronString: String, event: any.Any) extends Cron final case object Trigger extends Cron } case class CronState(event: Option[Cron.Create] = None) // Easy to imagine how this would be implemented def nextRun(cronStr: String): FiniteDuration = ??? def cron[F[_]: StatefulFunction[*[_], CronState]: Sync]( input: Cron ): F[Unit] = { val statefun = StatefulFunction[F, GreeterState] input match { case create @ Cron.Create(cronStr, _) =&gt; for { _ &lt;- statefun.modifyCtx(_.copy(Some(create))) id &lt;- statefun.functionId _ &lt;- statefun.sendDelayedMsg( \"example\", \"cron\", id, nextRun(cronStr) ) } yield () case Cron.Trigger =&gt; for { id &lt;- statefun.functionId state &lt;- statefun.getCtx create &lt;- Sync[F].fromOption(state.event, new RuntimeException(\"Event trigger was empty\")) _ &lt;- statefun.sendEgressMsg( \"example\", \"triggers\", KafkaProducerRecord( UUID.randomUUID().toString, create.event.toByteString, \"triggers\" ) ) _ &lt;- statefun.sendDelayedMsg( \"example\", \"cron\", id, nextRun(create.cronStr) ) } yield () } } This is a basic idea for how one could implement a cron scheduler using delayed messages. Users who want to create a Cron would send a message to cron with the ID of the job name (or some unique identifier), which would create some state to store the event we are using cron to trigger on a schedule. The function will simply continuously sends itself delayed messages for the next run time and publish the triggered event exactly once to Kafka."
    }    
  ];

  idx = lunr(function () {
    this.ref("title");
    this.field("content");

    docs.forEach(function (doc) {
      this.add(doc);
    }, this);
  });

  docs.forEach(function (doc) {
    docMap.set(doc.title, doc.url);
  });
}

// The onkeypress handler for search functionality
function searchOnKeyDown(e) {
  const keyCode = e.keyCode;
  const parent = e.target.parentElement;
  const isSearchBar = e.target.id === "search-bar";
  const isSearchResult = parent ? parent.id.startsWith("result-") : false;
  const isSearchBarOrResult = isSearchBar || isSearchResult;

  if (keyCode === 40 && isSearchBarOrResult) {
    // On 'down', try to navigate down the search results
    e.preventDefault();
    e.stopPropagation();
    selectDown(e);
  } else if (keyCode === 38 && isSearchBarOrResult) {
    // On 'up', try to navigate up the search results
    e.preventDefault();
    e.stopPropagation();
    selectUp(e);
  } else if (keyCode === 27 && isSearchBarOrResult) {
    // On 'ESC', close the search dropdown
    e.preventDefault();
    e.stopPropagation();
    closeDropdownSearch(e);
  }
}

// Search is only done on key-up so that the search terms are properly propagated
function searchOnKeyUp(e) {
  // Filter out up, down, esc keys
  const keyCode = e.keyCode;
  const cannotBe = [40, 38, 27];
  const isSearchBar = e.target.id === "search-bar";
  const keyIsNotWrong = !cannotBe.includes(keyCode);
  if (isSearchBar && keyIsNotWrong) {
    // Try to run a search
    runSearch(e);
  }
}

// Move the cursor up the search list
function selectUp(e) {
  if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index) && (index > 0)) {
      const nextIndexStr = "result-" + (index - 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Move the cursor down the search list
function selectDown(e) {
  if (e.target.id === "search-bar") {
    const firstResult = document.querySelector("li[id$='result-0']");
    if (firstResult) {
      firstResult.firstChild.focus();
    }
  } else if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index)) {
      const nextIndexStr = "result-" + (index + 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Search for whatever the user has typed so far
function runSearch(e) {
  if (e.target.value === "") {
    // On empty string, remove all search results
    // Otherwise this may show all results as everything is a "match"
    applySearchResults([]);
  } else {
    const tokens = e.target.value.split(" ");
    const moddedTokens = tokens.map(function (token) {
      // "*" + token + "*"
      return token;
    })
    const searchTerm = moddedTokens.join(" ");
    const searchResults = idx.search(searchTerm);
    const mapResults = searchResults.map(function (result) {
      const resultUrl = docMap.get(result.ref);
      return { name: result.ref, url: resultUrl };
    })

    applySearchResults(mapResults);
  }

}

// After a search, modify the search dropdown to contain the search results
function applySearchResults(results) {
  const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
  if (dropdown) {
    //Remove each child
    while (dropdown.firstChild) {
      dropdown.removeChild(dropdown.firstChild);
    }

    //Add each result as an element in the list
    results.forEach(function (result, i) {
      const elem = document.createElement("li");
      elem.setAttribute("class", "dropdown-item");
      elem.setAttribute("id", "result-" + i);

      const elemLink = document.createElement("a");
      elemLink.setAttribute("title", result.name);
      elemLink.setAttribute("href", result.url);
      elemLink.setAttribute("class", "dropdown-item-link");

      const elemLinkText = document.createElement("span");
      elemLinkText.setAttribute("class", "dropdown-item-link-text");
      elemLinkText.innerHTML = result.name;

      elemLink.appendChild(elemLinkText);
      elem.appendChild(elemLink);
      dropdown.appendChild(elem);
    });
  }
}

// Close the dropdown if the user clicks (only) outside of it
function closeDropdownSearch(e) {
  // Check if where we're clicking is the search dropdown
  if (e.target.id !== "search-bar") {
    const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
    if (dropdown) {
      dropdown.classList.remove("show");
      document.documentElement.removeEventListener("click", closeDropdownSearch);
    }
  }
}
