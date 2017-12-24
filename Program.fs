open Akka
open System.Threading
open Akka.Actor
open Akka.Configuration
open Akka.Dispatch.SysMsg
open Akka.Event
open Akka.Routing
open Akka.Streams
open Akka.Streams.Dsl
open Akka.Streams.Supervision
open Akkling
open Akkling.Streams
open Akkling.Streams.Operators
open System
open System.Net
open System.Threading.Tasks
open FSharp.Control.WebExtensions

module Graph =
    let mapMaterializedValue fn (graph: #IRunnableGraph<_>) =
        graph.MapMaterializedValue(Func<_,_> fn)

module Flow =
  open Akka.Streams.Stage

  type Logic<'i, 's, 'o, 'e>
    (
        shape: Shape, 
        in1: Inlet<'i * 's>, 
        out1: Outlet<Result<'o, 'e>>, 
        in2: Inlet<Result<'o, 'e> * 's>, 
        out2: Outlet<'i * 's>,
        retryWith
    ) 
    as self =
    inherit GraphStageLogic(shape)
    
    let mutable elementInCycle = false
    let mutable pending: (Result<'o, 'e> * 's) option = None
          
    do self.SetHandler(in1, 
        { new InHandler() with
            override __.OnPush() =
              let is' = self.Grab'(in1)
              if not (self.HasBeenPulled' in2) then self.Pull'(in2)
              self.Push'(out2, is')
              elementInCycle <- true
              
            override __.OnUpstreamFinish() =
              if not elementInCycle then
                self.CompleteStage() })
                
       self.SetHandler(out1, 
         { new OutHandler() with
             override __.OnPull() =
               if self.IsAvailable'(out2) then self.Pull'(in1)
               else self.Pull'(in2) })
  
       self.SetHandler(in2, 
         { new InHandler() with
             override __.OnPush() =
               elementInCycle <- false
               match self.Grab'(in2) with
               | ((Ok _) as success, _) -> self.PushAndCompleteIfLast(success)
               | ((Error _) as failure, s) -> 
                    match retryWith s with 
                    | None -> self.PushAndCompleteIfLast(failure)
                    | Some is ->
                        self.Pull'(in2)
                        if self.IsAvailable'(out2) then
                            self.Push'(out2, is)
                            elementInCycle <- true
                        else 
                            pending <- Some is })
  
       self.SetHandler(out2, 
         { new OutHandler() with 
             override __.OnPull() =
               if self.IsAvailable'(out1) && not elementInCycle then
                 match pending with
                 | Some p ->
                     self.Push'(out2, p)
                     pending <- None
                     elementInCycle <- true
                 | None ->
                     if not (self.HasBeenPulled'(in1)) then
                         self.Pull'(in1) 
                         
             override __.OnDownstreamFinish() = () })
      
    member private this.Grab' (i: Inlet) = this.Grab i
    member private this.HasBeenPulled' (i: Inlet) = this.HasBeenPulled i
    member private this.Push'(o: Outlet, y) = this.Push(o, y)
    member private this.Pull' (x: Inlet) = this.Pull x
    member private this.IsAvailable' (o: Outlet) = this.IsAvailable o
    member private this.IsAvailable' (i: Inlet) = this.IsAvailable i
          
    member private this.PushAndCompleteIfLast(elem: Result<'o, 'e>) =
      this.Push(out1, elem)
      if this.IsClosed in1 then
        this.CompleteStage()
        

  type RetryCoordinator<'i, 's, 'o, 'e>(retryWith: 's -> (Result<'o, 'e> * 's) option) =
    inherit GraphStage<BidiShape<'i * 's, Result<'o, 'e>, Result<'o, 'e> * 's, 'i * 's>>()
        
    let in1 = Inlet<'i * 's>("Retry.ext.in")
    let out1 = Outlet<Result<'o, 'e>>("Retry.ext.out")
    let in2 = Inlet<Result<'o, 'e> * 's>("Retry.int.in")
    let out2 = Outlet<'i * 's>("Retry.int.out")
    
    override __.Shape = BidiShape(in1, out1, in2, out2)
    
    override this.CreateLogic(_: Attributes) = Logic(this.Shape, in1, out1, in2, out2, retryWith) :> _
    
  let retryAsGraph<'i, 'o, 's, 'e, 'm> 
        (retryWith: 's -> (Result<'o, 'e> * 's) option) 
        (flow: IGraph<FlowShape<'i * 's, Result<'o, 'e> * 's>, 'm>) 
        : IGraph<FlowShape<'i * 's, Result<'o, 'e>>, 'm> =
    
    flow |> Graph.create1 (fun b origFlow ->
      let retry = b.Add(RetryCoordinator<'i, 's, 'o, 'e>(retryWith))
      b.From(retry.Outlet2).Via(origFlow).To(retry.Inlet2) |> ignore
      FlowShape(retry.Inlet1, retry.Outlet1)
    )
    
  let retry retryWith flow = 
    flow
    |> Flow.FromGraph
    |> retryAsGraph retryWith 
    |> Flow.FromGraph

[<EntryPoint>]
let main _ = 
    let config = 
        ConfigurationFactory.ParseString """
akka {
  remote.dot-netty.tcp {
    transport-class = ""Akka.Remote.Transport.DotNetty.DotNettyTransport, Akka.Remote""
    transport-protocol = tcp
    port = 8091
    hostname = ""127.0.0.1""
  }
  actor { 
    serializers {
      wire = "Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion"
    }
    serialization-bindings {
      "System.Object" = wire
    }
    debug {  
      receive = off
      autoreceive = off
      lifecycle = off
      event-stream = off
      unhandled = on
    }
  }
  stdout-loglevel = DEBUG
  loglevel = DEBUG
}"""
    let system = ActorSystem.Create("test", config)
    let mat = 
        ActorMaterializerSettings
            .Create(system)
            //.WithSupervisionStrategy(Deciders.RestartingDecider)
        |> system.Materializer
        
    let flow : Flow<int, Result<int, string>, NotUsed> = 
        Flow.id 
        |> Flow.map (function
            | 6 | 7 | 8 -> Result.Error "It's 6 and we are failing"
            | x -> Result.Ok x) 
        
    let restartFlow =
        RestartFlow.WithBackoff(
            (fun _ -> flow), TimeSpan.FromSeconds 1., TimeSpan.FromSeconds 5., 0.2)
        
    let retryFlow =
        restartFlow
        |> Flow.map (fun x -> x, 0)
        |> Flow.retry (fun s -> None)
//             if s < 3 then 
//                Some (Result.Error (sprintf "attempt %d failed" s), s + 1)
//             else 
                //None)
        
    async {
        do! [1..10]
            |> Source.ofList
            |> Source.via restartFlow
            |> Source.map (fun x -> x, 0)
            |> Source.via (Flow.retry (fun s -> None) Flow.id)
            |> Source.runWith mat (Sink.forEach (printfn "%A"))
        
        do! system.Terminate() |> Async.AwaitTask
    } 
    |> Async.RunSynchronously
        
    Task.WaitAll system.WhenTerminated
        
//    let urls = 
//        [ "http://ya.ru"
//          "foo-bar"
//          "http://foo.com.com"
//          "http://www.y234.ru"
//          "http://google.com" ]
//        |> Source.ofList
//        
//    let doDownload =
//        RestartFlow.WithBackoff((fun _ -> 
//            Flow.id
//            |> Flow.asyncMapUnordered 10 (fun uri ->
//                 async {
//                    printfn "~~~~~~~~ try %O..." uri
//                    use client = new WebClient()
//                    let! content = client.AsyncDownloadString uri
//                    return uri, content.[..50]
//                 })),
//            TimeSpan.FromSeconds 1.,
//            TimeSpan.FromSeconds 1.,
//            0.2)
//    
//    let download =
//        let flow =
//            Flow.id 
//            |> Flow.map Uri
//            |> Flow.log "Uri"
//            |> Flow.via doDownload
//            |> Flow.log "Download"
//        flow.WithAttributes(Attributes.CreateLogLevels(onError = LogLevel.ErrorLevel))
//        
//    async {
//        do! urls 
//            |> Source.via download 
//            |> Source.runForEach mat (printfn "%A")
//        do! system.Terminate() |> Async.AwaitTask
//    } |> Async.RunSynchronously
        
//    Task.WaitAll system.WhenTerminated
    0
