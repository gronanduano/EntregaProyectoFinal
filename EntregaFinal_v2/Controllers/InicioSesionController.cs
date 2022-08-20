using Microsoft.AspNetCore.Mvc;
using Usuarios.Models;
using UsuarioHandlers;
using Usuarios.DTOS;

namespace InicioSesionControllers
{
    [ApiController]
    [Route("[controller]")]

    public class InicioSesionController : ControllerBase
    {
        [HttpGet(Name = "InicioSesion")]
        public Usuario InicioSesion([FromHeader] string user, string psw)
        {
            return UsuarioHandler.ValidarAccesoUsuario(user, psw);
        }
    }
}
