using Microsoft.AspNetCore.Mvc;
using Ventas.Models;
using VentaHandlers;
using Ventas.DTOS;

namespace VentaControllers
{
    [ApiController]
    [Route("[controller]")]

    public class VentaController : ControllerBase
    {
        [HttpGet(Name = "GetVenta")]

        public List<Venta> ObtenerVentas([FromHeader] int IDUsuario)
        {
            return VentaHandler.BuscarVentasTotales(IDUsuario);
        }
        
        [HttpPost(Name = "PostVenta")]
        public List<PostVenta> InsertarVentas([FromBody] List<PostVenta> ListVenta)
        {
            return VentaHandler.InsertarVentas(ListVenta);
        }
        
        [HttpDelete(Name = "DeleteVenta")]
        public string EliminarVenta([FromHeader] int venta)
        {
            return VentaHandler.EliminarVenta(venta);
        }

    }
}