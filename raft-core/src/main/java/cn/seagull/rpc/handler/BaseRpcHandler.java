package cn.seagull.rpc.handler;

import cn.hutool.core.collection.CollectionUtil;
import cn.seagull.rpc.Connection;
import cn.seagull.rpc.InvokeFuture;
import cn.seagull.rpc.RpcCommand;
import cn.seagull.rpc.impl.DefaultRpcCommand;
import cn.seagull.util.ServiceLoaderFactory;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author yupeng
 */
@Slf4j
public class BaseRpcHandler
{
    private ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();;
    private List<RpcHandler> rpcHandlers = ServiceLoaderFactory.loadService(RpcHandler.class);
    private static BaseRpcHandler INSTANCE = new BaseRpcHandler();

    public static BaseRpcHandler getInstance()
    {
        return INSTANCE;
    }

    public void handler(ChannelHandlerContext ctx, RpcCommand rpcCommand) throws Exception
    {
        if (CollectionUtil.isEmpty(rpcHandlers)) {
            return;
        }

        executorService.submit(new Runnable()
        {
            @Override
            public void run()
            {
                Object command = rpcCommand.command();
                Long invokedId = rpcCommand.invokeId();
                Connection conn = ctx.channel().attr(Connection.CONNECTION).get();

                for (RpcHandler handler : rpcHandlers)
                {
                    if (command != null && handler.isAssignableFrom(command.getClass())) {
                        Object process = handler.process(command);
                        ctx.channel().writeAndFlush(new DefaultRpcCommand(invokedId, process));
                    }
                }

                if (conn != null) {
                    InvokeFuture invokeFuture = conn.removeInvokeFuture(invokedId);
                    if (invokeFuture != null) {
                        invokeFuture.setResult(command);
                    }
                }
            }
        });
    }
}
