defmodule AlgorithmRunner do
  def main(args\\[]) do
    [x, structure, alg] = for arg <- args, do: String.trim(arg)
    no_nodes = x |> String.to_integer

    no_nodes = case structure do
      "3Dtorus" -> round(:math.pow(Float.ceil(:math.pow(no_nodes,(1/3))),3))
      "honeycomb" -> round(:math.pow(Float.ceil(:math.sqrt(no_nodes)), 2))
      _ -> no_nodes
    end

    case alg do
      "gossip" ->
            actors = AlgorithmRunner.init_actors_gsp(no_nodes)
            AlgorithmRunner.start_msging(actors, structure, no_nodes, alg)

      "push-sum" ->
            actors = AlgorithmRunner.init_actors_psum(no_nodes)
            AlgorithmRunner.start_msging(actors, structure, no_nodes, alg)
    end
  end

  def init_actors_gsp(no_nodes) do
    rand = Enum.random(1..(no_nodes))
    Enum.map(1..no_nodes,
    fn x ->
      {:ok, actor} = cond do
      x == rand -> UtilFunctions.start_link("I am Lokesh!")
      true -> UtilFunctions.start_link("")
    end
    actor end)
  end

  def verify_alive_psum(pids) do
    current_actors = Enum.map(pids,
        fn pid ->
          diff = UtilFunctions.get_diff(pid)
          if(Process.alive?(pid) && UtilFunctions.has_nbrs(pid) && (abs(List.first(diff)) > :math.pow(10, -10)
                 || abs(List.last(diff)) > :math.pow(10, -10))) do
             pid
          end
        end)
    List.delete(Enum.uniq(current_actors), nil)
  end

  def start_msging(actors, structure, no_nodes, algo) do
    :ets.new(:count, [:set, :public, :named_table])
    :ets.insert(:count, {"spread", 0})

    {:ok, topo_server_pid}=Structure.start_link([structure,actors])
    nbrs = Structure.get_neighbors(topo_server_pid)

    for {k,v} <- nbrs do
      UtilFunctions.set_nbrs(k,v)
    end

    prev = System.monotonic_time(:millisecond)

    if (algo == "gossip") do
      gsp_algo(actors, nbrs, no_nodes)
    else
      psum_algo(actors, nbrs, no_nodes)
    end
    IO.puts "Time Taken: " <> to_string(System.monotonic_time(:millisecond) - prev) <> " milliseconds"
  end

  def init_actors_psum(no_nodes) do
    rand = Enum.random(1..(no_nodes))
    Enum.map(1..no_nodes,
    fn x ->
      {:ok, actor} = cond do
        x == rand ->
          UtilFunctions.start_link({x,"I am Lokesh"})
        true ->
          UtilFunctions.start_link({x,""})
      end
      actor
    end)
  end

  def psum_algo(actrs, nbrs, no_nodes) do
    for {k, _v} <- nbrs do
      UtilFunctions.sendmsg_psum(k)
    end

    actrs = verify_alive_psum(actrs)
    num_of_actors = length(actrs)
    [{_, spread}] = :ets.lookup(:count, "spread")

    if ((spread/no_nodes) < 0.95 && length(actrs) > 1) do
      nbrs = Enum.filter(nbrs, fn ({k,_}) -> Enum.member?(actrs, k) end)
      [{_, spread}] = :ets.lookup(:count, "spread")
      psum_algo(actrs, nbrs, no_nodes)
    # else
    #   #IO.puts "Spread: " <> to_string(spread * 100/no_nodes) <> " %"

    end
  end

  def verify_alive(pids) do
    current_actors = Enum.map(pids, fn pid -> if (Process.alive?(pid) && UtilFunctions.retrieve_cnt(pid) < 10  && UtilFunctions.has_nbrs(pid)) do pid end end)
    List.delete(Enum.uniq(current_actors), nil)
  end

  def gsp_algo(actr, nbrs, no_nodes) do
    for {k, _v}  <-  nbrs  do
      UtilFunctions.send_msg(k)
    end

    actr = verify_alive(actr)
    num_of_actors = length(actr)
    [{_, spread}] = :ets.lookup(:count, "spread")

    if ((spread/no_nodes) < 0.9 && length(actr) > 1) do
      nbrs = Enum.filter(nbrs, fn {k,_} -> Enum.member?(actr, k) end)
      gsp_algo(actr, nbrs, no_nodes)
    # else
    #   IO.puts "Spread: " <> to_string(spread * 100/no_nodes) <> "%"
    end
  end

end

defmodule UtilFunctions do
  use GenServer

  def set_nbrs(server_pid, neighbors) do
    GenServer.cast(server_pid, {:set_nbrs, neighbors})
  end

  def retrieve_cnt(pid) do
    cnt = GenServer.call(pid, {:get_cnt})
    cnt
  end

  def sendmsg_psum(pid) do
    GenServer.cast(pid, {:snd_msg_psum})
  end

  def get_neighbors(pid) do
    GenServer.call(pid, {:get_nbrs})
  end

  def has_nbrs(pid) do
    {:ok, nbrs} = GenServer.call(pid, {:get_nbrs})
    length(nbrs)>0
  end

  def get_diff(pid) do
    GenServer.call(pid, {:get_diff})
  end

  def start_link(arg) do
      GenServer.start_link(Server, arg)
  end

  def send_msg(pid) do
    GenServer.cast(pid, {:snd_msg})
  end

end

defmodule Server do
  use GenServer

  def init(args) do
    if is_tuple(args) do
      {:ok, %{"s" => elem(args,0), "rumor" => elem(args,1), "w"=> 1, "s_old"=>1, "w_old"=>1, "diff" => 1, "diff_old" => 1, "neighbors" => []}}
    else
      {:ok, %{"rumor" => args, "count" => 0, "neighbors" => []}}
    end
  end

  def handle_cast({:set_nbrs, nbrs}, state) do
      {:noreply, Map.put(state, "neighbors", nbrs)}
  end

  def handle_call({:get_cnt}, _from, state) do
    {:ok, cnt} = Map.fetch(state, "count")
    {:reply,cnt, state}
  end

  def handle_cast({:rcv_msg, message, snd_id}, state) do
    {:ok, count} = Map.fetch(state, "count")
    count = count + 1
    state = Map.put(state, "count", count)

    if (count == 10) do
      [{_, spread}] = :ets.lookup(:count, "spread")
      :ets.insert(:count, {"spread", spread + 1})
    end

    if (count > 10) do
      _ = GenServer.cast(snd_id, {:rmv_nbr, self()})
      {:noreply, state}
   else
       {:ok, cur_rmr} = Map.fetch(state, "rumor")

       if(cur_rmr != "") do
           {:noreply, state}
       else
           state = Map.put(state, "rumor", message)
           {:noreply, state}
       end
   end
  end

  def handle_cast({:snd_msg}, state) do
    {:ok, rmr} = Map.fetch(state, "rumor")
    {:ok, nbrs} = Map.fetch(state, "neighbors")
    if (rmr != "" && length(nbrs) > 0) do
        neighbor = Enum.random(nbrs)
        _ = GenServer.cast(neighbor, {:rcv_msg, rmr, self()})
    end
    {:noreply, state}
  end

  def handle_cast({:rmv_nbr, neighbor}, state) do
    {:ok, nbrs} = Map.fetch(state, "neighbors")
    state = Map.put(state, "neighbors", List.delete(nbrs, neighbor))
    {:noreply, state}
  end

  def handle_call({:get_nbrs}, _from, state) do
    nbrs =  Map.fetch(state, "neighbors")
    {:reply,nbrs, state}
  end

  def handle_call({:get_diff}, _from, state) do
    {:ok, dff} = Map.fetch(state, "diff")
    {:ok, dff_old} = Map.fetch(state, "diff_old")
    {:reply, [dff] ++ [dff_old], state}
  end

  def handle_cast({:rcv_msg_psum, sender_pid, s, w, message}, state) do
    {:ok, w_current} = Map.fetch(state, "w")
    {:ok, exist_rmr} = Map.fetch(state, "rumor")
    {:ok, d} = Map.fetch(state, "diff")
    {:ok, dold} = Map.fetch(state, "diff_old")
    {:ok, s_old} = Map.fetch(state, "s_old")
    {:ok, w_old} = Map.fetch(state, "w_old")
    {:ok, s_current} = Map.fetch(state, "s")

    s_new = s_current + s
    w_new = w_current + w

    if (abs(s_new/w_new - s_current/w_current) < :math.pow(10, -10) && abs(s_current/w_current - s_old/w_old) < :math.pow(10, -10)) do
      GenServer.cast(sender_pid, {:rmv_nbr, self()})
    else
      if(exist_rmr == "") do
        state = Map.put(state, "rumor", message)
        [{_, spread}] = :ets.lookup(:count, "spread")
        :ets.insert(:count, {"spread", spread + 1})
      end
      state = Map.put(state, "w", w_new)
      state = Map.put(state, "diff", s_new/w_new - s_current/w_current)
      state = Map.put(state, "diff_old", s_current/w_current - s_old/w_old)
      state = Map.put(state, "s_old", s_current)
      state = Map.put(state, "w_old", w_current)
      state = Map.put(state, "s", s_new)
    end
    {:noreply, state}
  end

  def handle_cast({:snd_msg_psum}, state) do
    {:ok, w_current} = Map.fetch(state, "w")
    {:ok, d} = Map.fetch(state, "diff")
    {:ok, dold} = Map.fetch(state, "diff_old")
    {:ok, rmr} = Map.fetch(state, "rumor")
    {:ok, nbrs} = Map.fetch(state, "neighbors")
    {:ok, s_current} = Map.fetch(state, "s")

    if (rmr != "" && length(nbrs) > 0) do
      w_current = w_current/2
      s_current = s_current/2
      state = Map.put(state, "w", w_current)
      state = Map.put(state, "s", s_current)
      neighbor = Enum.random(nbrs)
      GenServer.cast(neighbor, {:rcv_msg_psum, self(), s_current, w_current, rmr})
    end
    {:noreply, state}
  end

end

AlgorithmRunner.work(System.argv())

